/*
 * main.cpp
 *
 *  Created on: 9. 1. 2021
 *      Author: ondra
 */

#include <chrono>
#include <cmath>
#include <csignal>
#include <map>

#include <imtjson/value.h>
#include <imtjson/object.h>
#include <imtjson/string.h>
#include <imtjson/serializer.h>
#include <imtjson/parser.h>
#include "../docdb/src/docdblib/aggregator_view.h"
#include "../docdb/src/docdblib/db.h"
#include "../docdb/src/docdblib/json_map.h"
#include "../shared/default_app.h"
#include "../shared/linux_crash_handler.h"
#include "../shared/logOutput.h"
#include "../userver/http_server.h"
#include "../userver/query_parser.h"

using ondra_shared::logInfo;
using ondra_shared::logWarning;
using ondra_shared::logFatal;
using ondra_shared::logNote;

using namespace docdb;

static constexpr std::size_t daysec = 24*60*60;

static AsyncProvider asyncProvider;

template<typename Source>
static bool generateData(Source &pmap, PHttpServerRequest &req, const std::string_view &vpath, unsigned int timeMult) {
	if (req->getMethod() == "GET") {
		QueryParser qp(vpath);
		auto asset=qp["asset"];
		auto currency=qp["currency"];
		auto from=qp["from"].getUInt();
		auto to=qp["to"].getUInt();
		if (to == 0) --to;

		req->setContentType("application/json");
		auto s = req->send();
		s.putChar('[');
		bool comma = false;
		char buffer[200];
		if (asset == "usd") {
			auto iter1 = pmap.range({currency, from},{currency, to});
			while (iter1.next()) {
				auto t1 = iter1.key(1).getUInt();
				double v1 = iter1.value().getNumber();
				if (comma) {
					s.write(",\r\n");
				} else {
					comma = true;
				}
				snprintf(buffer,200,"[%lu, %g]", t1*timeMult, 1.0/v1);
				s.write(buffer);
			}
		} else if (currency == "usd") {
			auto iter1 = pmap.range({asset, from},{asset, to});
			while (iter1.next()) {
				auto t1 = iter1.key(1).getUInt();
				double v1 = iter1.value().getNumber();
				if (comma) {
					s.write(",\r\n");
				} else {
					comma = true;
				}
				snprintf(buffer,200,"[%lu, %g]", t1*timeMult, v1);
				s.write(buffer);
			}
		} else {
			auto iter1 = pmap.range({asset, from},{asset, to});
			auto iter2 = pmap.range({currency, from},{currency, to});
			bool rep_iter1 = false;
			bool rep_iter2 = false;
			while ((rep_iter1 || iter1.next()) && (rep_iter2 || iter2.next())) {
				rep_iter1 = false;
				rep_iter2 = false;
				auto t1 = iter1.key(1).getUInt();
				auto t2 = iter2.key(1).getUInt();
				if (t1 < t2) rep_iter2 = true;
				else if (t1 > t2) rep_iter1 = true;
				else {
					double v1 = iter1.value().getNumber();
					double v2 = iter2.value().getNumber();
					if (comma) {
						s.write(",\r\n");
					} else {
						comma = true;
					}
					snprintf(buffer,200,"[%lu, %g]", t1*timeMult, v1/v2);
					s.write(buffer);
				}
			}
		}
		s.putChar(']');
		s.flush();
		return true;
	} else {
		return false;
	}
}

class MyHttpServer: public HttpServer {
public:
	MyHttpServer():lo("http") {}

	virtual void log(ReqEvent event, const HttpServerRequest &req) {
		if (event == ReqEvent::done) {
			auto now = std::chrono::system_clock::now();
			auto dur = std::chrono::duration_cast<std::chrono::microseconds>(now-req.getRecvTime());
			char buff[100];
			snprintf(buff,100,"%1.3f ms", dur.count()*0.001);
			lo.progress("#$1 $2 $3 $4 $5 $6", req.getIdent(), req.getStatus(), req.getMethod(), req.getHost(), req.getURI(), buff);
		}
	}
	virtual void log(const HttpServerRequest &req, const std::string_view &msg) {
		lo.note("#$1 $2", req.getIdent(), msg);
	}
protected:
	ondra_shared::LogObject lo;

};

int main(int argc, char **argv) {

	ondra_shared::DefaultApp app({},std::cerr);
	if (!app.init(argc, argv)) {
		std::cerr << "Invalid arguments" << std::endl;
		return EINVAL;
	}

	logNote("---- START ----");
	ondra_shared::CrashHandler crashHandler([](const char *c){
		logFatal("CrashHandler: $1", c);
	});
	crashHandler.install();

	auto server_section = app.config["server"];
	auto db_section = app.config["db"];
	auto www_section = app.config["www"];



	Config cfg;
	cfg.write_buffer_size = db_section.mandatory["write_buffer_size_mb"].getUInt() * 1024 * 1024;
	cfg.max_file_size = db_section.mandatory["max_file_size_mb"].getUInt() * 1024 * 1024;
	cfg.block_cache = DB::createCache(db_section.mandatory["cache_size_mb"].getUInt() * 1024 * 1024);
	cfg.logger = [dblog = std::make_shared<ondra_shared::LogObject>("leveldb")](const char *str, va_list args) mutable {
			char buff[1024];
			int wr = vsnprintf(buff,sizeof(buff), str, args);
			while (wr && isspace(buff[wr-1])) wr--;
			dblog->info("$1", std::string_view(buff,wr));
	};

	std::string upload_host = www_section["upload_host"].getString();
	auto checkHost = [&](std::string_view host) {
		return host.find(upload_host) != host.npos;
	};

	DB db(db_section.mandatory["path"].getPath(), cfg);
	MyHttpServer server;

	JsonMap pmap(db,"pmap");
	AggregatorView<JsonMap::AggregatorAdapter> dailyPrice(pmap, "daily", [](json::Value key, IMapKey &mp){
		json::Value symb = key[0];
		std::size_t sec = key[1].getUInt();
		std::size_t day = sec/(daysec);
		std::size_t from = day*(daysec);
		std::size_t to = (day+1)*(daysec);
		mp.range({symb,day}, {symb, from}, {symb, to}, false, json::Value());
	}, [](JsonMap::Iterator &iter, const json::Value &) -> json::Value {
		if (!iter.next()) return json::Value();
		double z= iter.value().getNumber();
		unsigned int count = 1;
		while (iter.next()) {
			z+= iter.value().getNumber();
			count ++;
		}
		return z/count;
	});

	AggregatorView<decltype(dailyPrice)::AggregatorAdapter> totalRange(dailyPrice, "total", [](json::Value key, IMapKey &mp){
		json::Value symb = key[0];
		mp.prefix(symb, json::Value(json::array, {symb}), json::Value());
	},[](JsonMap::Iterator &iter, const json::Value &) ->json::Value {
		if (!iter.next()) return json::Value();
		std::uint64_t beg = iter.key(1).getUInt();
		std::uint64_t end = beg;
		std::uint64_t cnt = 1;
		while (iter.next()) {
			end = iter.key(1).getUInt();
			cnt++;
		}
		return {beg, end, cnt};
	});


	server.addPath("/import", [&](PHttpServerRequest &req, const std::string_view &vpath) mutable {
		if (req->getMethod() == "POST") {
			if (!checkHost(req->getHost())) {
				req->sendErrorPage(403);return true;
			}
			Stream b = req->getBody();
			json::Value v = json::Value::parse([&]()->int {
				return b.getChar();
			});
			json::Value rows = v["rows"];
			for (json::Value rw: rows) {
				json::Value doc = rw["doc"];
				std::uint64_t time = rw["id"].getUInt()*10;
				json::Value prices = doc["prices"];
				docdb::Batch b;
				for (json::Value c: prices) {
					pmap.set(b, {c.getKey(), time}, c.getNumber());
				}
				db.commitBatch(b);
			};
			req->sendErrorPage(202);
			return true;
		} else {
			return false;
		}
	});

	server.addPath("/symbols", [&](PHttpServerRequest &req, const std::string_view &vpath)mutable{
		if (req->getMethod() == "GET") {
			req->setContentType("application/json");;
			Stream s = req->send();
			s.putCharNB('{');
			bool comma = false;
			for (auto iter = totalRange.scan(); iter.next();) {
				if (comma) {
					s.write(",\r\n");
				} else {
					comma = true;
				}
				auto k = iter.key();
				auto v = iter.value();
				k.serialize([&](char c){s.putCharNB(c);});
				s.putChar(':');
				v.serialize([&](char c){s.putCharNB(c);});
			}
			s.putCharNB('}');
			s.flush();
			return true;
		} else {
			return false;
		}
	});

	server.addPath("/minute", [&](PHttpServerRequest &req, const std::string_view &vpath){
		return generateData(pmap, req, vpath,1);
	});
	server.addPath("/minute.php", [&](PHttpServerRequest &req, const std::string_view &vpath){
		if (vpath.size() < 2 && req->getMethod() == "GET") {
			req->setContentType("application/json");;
			Stream s = req->send();
			s.putCharNB('{');
			bool comma = false;
			for (auto iter = totalRange.scan(); iter.next();) {
				if (comma) {
					s.write(",\r\n");
				} else {
					comma = true;
				}
				auto k = iter.key();
				auto v = iter.value();
				k.serialize([&](char c){s.putCharNB(c);});
				s.putChar(':');
				json::Value(v[2].getUInt()*daysec/60).serialize([&](char c){s.putCharNB(c);});
			}
			s.putCharNB('}');
			s.flush();
			return true;
		} else {
			return generateData(pmap, req, vpath,1);
		}
	});
	server.addPath("/daily", [&](PHttpServerRequest &req, const std::string_view &vpath){
		return generateData(dailyPrice, req, vpath,daysec);
	});
	server.addPath("/history", [&](PHttpServerRequest &req, std::string_view vpath){
		if (req->getMethod() == "GET" && !vpath.empty()) {
			QueryParser qp(vpath);
			std::string_view path = qp.getPath();
			if (path[0] == '/') path = path.substr(1);
			json::Value at(std::strtoul(path.data(), nullptr, 10));
			bool comma = false;
			double divider = 1;
			auto cur = qp["currency"];
			if (cur.defined) {
				json::Value price = pmap.lookup({cur, at});
				if (!price.defined()) {
					req->sendErrorPage(404);
					return true;
				}
				divider = price.getNumber();
			}

			req->setContentType("application/json");;
			Stream s = req->send();
			s.putCharNB('{');

			for (auto iter = totalRange.scan(); iter.next();) {
				auto symbol = iter.key();
				json::Value v = pmap.lookup(json::Value({symbol,at}));
				if (v.defined()) {
					if (comma) {
						s.write(",\r\n");
					} else {
						comma = true;
					}
					v = json::Value(v.getNumber()/divider);
					symbol.serialize([&](char c){s.putCharNB(c);});
					s.putChar(':');
					v.serialize([&](char c){s.putCharNB(c);});
				}
			}
			s.putCharNB('}');
			s.flush();
			return true;
		} else {
			return false;
		}

	});

	std::map<std::string, std::pair<double,unsigned int> > symbolMap;
	std::mutex symbolMapLock;


	server.addPath("/collector", [&](PHttpServerRequest &req, std::string_view vpath){
		if (req->getMethod() == "POST") {
			if (!checkHost(req->getHost())) {
				req->sendErrorPage(403);return true;
			}
			auto curTime = ((std::chrono::duration_cast<std::chrono::seconds>(
					std::chrono::system_clock::now().time_since_epoch()).count()+30)/60)*60;
			json::Value body;
			if (req->isBodyAvailable()) {
				Stream b = req->getBody();
				body = json::Value::parse([&]()->int {
					return b.getChar();
				});
			}
			std::unique_lock _(symbolMapLock);
			if (vpath.empty()) {
				symbolMap.clear();
				auto cryptowatch_result = body[0]["result"];
				auto ftx_result = body[1]["result"];
				for (json::Value x: cryptowatch_result["rows"]) {
					 auto symbol = x["symbol"].toString();
					 auto price = x["price"].getNumber();
					 if (price && std::isfinite(price)) {
						 auto res = symbolMap.emplace(symbol.str(), std::pair{price,1} );
						 if (!res.second) {
							 res.first->second.first += price;
							 res.first->second.second ++;
						 }

					 }
				}
				for (json::Value x: ftx_result) {
					bool skip = false;
					double price = x["price"].getNumber();
					std::string symbol;
					if (x["type"].getString() == "future") {
						symbol=x["name"].getString();
						while (!symbol.empty() && symbol.back()>='0' && symbol.back()<='9')  {
							symbol.pop_back();
						}
						if (!symbol.empty() && symbol.back()=='-') {
							symbol.append("fut");
						} else {
							symbol=x["name"].getString();
						}
					} else {
						symbol=x["baseCurrency"].getString();
						skip=x["quoteCurrency"].getString() != "USD";
					}
					std::transform(symbol.begin(), symbol.end(),symbol.begin(),[](char c){return std::tolower(c);});

				  if (!skip && std::isfinite(price) && price) {
						 auto res = symbolMap.emplace(symbol, std::pair{price,1} );
						 if (!res.second) {
							 res.first->second.first += price;
							 res.first->second.second ++;
						 }
				  }
				}
				Batch batch;
				for (const auto &m: symbolMap) {
					pmap.set(batch, {m.first, curTime}, m.second.first/m.second.second);
				}
				db.commitBatch(batch);
				req->setStatus(202);
				req->send("ok");
				return true;
			} else if (vpath == "/commit") {
				Batch batch;
				for (const auto &m: symbolMap) {
					pmap.set(batch, {m.first, curTime}, m.second.first/m.second.second);
				}
				db.commitBatch(batch);
				req->log("Commit ", symbolMap.size(), " entries (timestamp: ",curTime,")");
				symbolMap.clear();
				req->setStatus(202);
				req->send("");
				return true;
			} else if (vpath == "/cryptowatch") {
				json::Value cryptowatch_result = body["result"];
				for (json::Value x: cryptowatch_result["rows"]) {
					 auto symbol = x["symbol"].toString();
					 auto price = x["price"].getNumber();
					 if (price && std::isfinite(price)) {
						 auto res = symbolMap.emplace(symbol.str(), std::pair{price,1} );
						 if (!res.second) {
							 res.first->second.first += price;
							 res.first->second.second ++;
						 }

					 }
				}
				req->setStatus(202);
				req->send("");
				return true;
			} else if (vpath == "/ftx") {
				auto ftx_result = body["result"];
				for (json::Value x: ftx_result) {
					bool skip = false;
					double price = x["price"].getNumber();
					std::string symbol;
					if (x["type"].getString() == "future") {
						symbol=x["name"].getString();
						while (!symbol.empty() && symbol.back()>='0' && symbol.back()<='9')  {
							symbol.pop_back();
						}
						if (!symbol.empty() && symbol.back()=='-') {
							symbol.append("fut");
						} else {
							symbol=x["name"].getString();
						}
					} else {
						symbol=x["baseCurrency"].getString();
						skip=x["quoteCurrency"].getString() != "USD";
					}
					std::transform(symbol.begin(), symbol.end(),symbol.begin(),[](char c){return std::tolower(c);});

				  if (!skip && std::isfinite(price) && price) {
						 auto res = symbolMap.emplace(symbol, std::pair{price,1} );
						 if (!res.second) {
							 res.first->second.first += price;
							 res.first->second.second ++;
						 }
				  }
				}
				req->setStatus(202);
				req->send("");
				return true;
			} else {
				req->sendErrorPage(404);
				return true;
			}
		} else {
			return false;
		}
	});
	server.addPath("/compact", [&](PHttpServerRequest &req, std::string_view ){
		if (req->getMethod()=="POST" ) {
			if (!checkHost(req->getHost())) {
				req->sendErrorPage(403);return true;
			}
			req->setStatus(202);
			req->setContentType("text/plain");
			Stream s = req->send();
			s.write("Started\r\n");
			s.flush();
			db.compact();
			s.write("Finished\r\n");
			s.flush();
			return true;
		} else {
			return false;
		}
	});
	std::string docroot = www_section.mandatory["document_root"].getPath();
	server.addPath("", [&](PHttpServerRequest &req, std::string_view vpath){
		auto pos = vpath.find('?');
		if (pos != vpath.npos) {
			vpath = vpath.substr(0,pos);
		}
		if (vpath.find('/',1) != vpath.npos) return false;
		std::string fname(docroot);
		if (vpath=="/") vpath = "/index.html";
		fname.append(vpath);
		return req->sendFile(std::move(req), fname);
	});

	server.start(NetAddr::fromString(server_section.mandatory["listen"].getString(), "3456"),
			server_section.mandatory["threads"].getUInt(), 1);

	asyncProvider = server.getAsyncProvider();

	auto stopServer = [](int) {
		asyncProvider->stop();
	};

	signal(SIGINT, stopServer);
	signal(SIGTERM, stopServer);

	server.addThread();
	server.stop();





}
