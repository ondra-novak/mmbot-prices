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
#include "../docdb/src/docdblib/inspector.h"
#include "../docdb/src/docdblib/json_map.h"
#include "../shared/default_app.h"
#include "../shared/linux_crash_handler.h"
#include "../shared/logOutput.h"
#include "../userver/dgramsocket.h"
#include "../userver/http_client.h"
#include "../userver/openapi.h"
#include "../userver/query_parser.h"

using ondra_shared::logInfo;
using ondra_shared::logWarning;
using ondra_shared::logFatal;
using ondra_shared::logNote;

using namespace docdb;
using namespace userver;

static constexpr std::size_t daysec = 24*60*60;

static AsyncProvider asyncProvider;

template<typename Source, typename Fn>
static void iterateData(Source &pmap, std::string_view asset, std::string_view currency, std::uint64_t from, std::uint64_t to, std::uint64_t timeMult, Fn &&out) {
	if (to == 0) --to;
	if (asset == "usd") {
		auto iter1 = pmap.range({currency, from},{currency, to});
		while (iter1.next()) {
			auto t1 = iter1.key(1).getUInt();
			double v1 = iter1.value().getNumber();
			out(t1*timeMult, 1.0/v1);
		}
	} else if (currency == "usd") {
		auto iter1 = pmap.range({asset, from},{asset, to});
		while (iter1.next()) {
			auto t1 = iter1.key(1).getUInt();
			double v1 = iter1.value().getNumber();
			out(t1*timeMult, v1);
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
				out(t1*timeMult, v1/v2);
			}
		}
	}
}

template<typename Source>
static bool generateData(Source &pmap, PHttpServerRequest &req, const RequestParams &qp, unsigned int timeMult) {
	if (req->getMethod() == "GET") {
		auto asset=qp["asset"];
		auto currency=qp["currency"];
		auto from=qp["from"].getUInt();
		auto to=qp["to"].getUInt();

		req->setContentType("application/json");
		auto s = req->send();
		s.putChar('[');
		bool comma = false;
		char buffer[200];
		iterateData(pmap, asset, currency, from, to, timeMult, [&](std::uintptr_t t1, double v1){
			if (comma) {
				s.write(",\r\n");
			} else {
				comma = true;
			}
			snprintf(buffer,200,"[%lu, %g]", t1, v1);
			s.write(buffer);
		});
		s.putChar(']');
		s.flush();
		return true;
	} else {
		return false;
	}
}

class MyHttpServer: public OpenAPIServer {
public:
	MyHttpServer():lo("http") {}

	virtual void log(ReqEvent event, const HttpServerRequest &req) override {
		if (event == ReqEvent::done) {
			std::lock_guard _(mx);
			auto now = std::chrono::system_clock::now();
			auto dur = std::chrono::duration_cast<std::chrono::microseconds>(now-req.getRecvTime());
			char buff[100];
			snprintf(buff,100,"%1.3f ms", dur.count()*0.001);
			lo.progress("#$1 $2 $3 $4 $5 $6", req.getIdent(), req.getStatus(), req.getMethod(), req.getHost(), req.getPath(), buff);
		}
	}
	virtual void log(const HttpServerRequest &req, const std::string_view &msg) override {
		std::lock_guard _(mx);
		lo.note("#$1 $2", req.getIdent(), msg);
	}
	virtual void unhandled() override {
		try {
			throw;
		} catch (std::exception &e) {
			std::lock_guard _(mx);
			lo.error("Unhandled exception: $1", e.what());
		} catch (...) {

		}
	}
protected:
	std::mutex mx;
	ondra_shared::LogObject lo;

};

template<typename T>
void updatePrice(T &iter, double price) {
	if (iter.second == 0) {
		iter.first = price;
		iter.second = 1;
	} else {
		iter.first += price;
		iter.second ++;
	}
}

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
	cfg.logger = [dblog = std::make_shared<ondra_shared::LogObject>("leveldb")](std::string_view txt) mutable {
			dblog->info("$1", txt);
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


	server.setInfo({
		"Crypto Prices API","1.0","Crypto Prices API","","Ondrej Novak","","nov.ondrej@gmail.com"
	});

	server.addPath("/import")
		.POST("Admin","Import data from couchdb databse","",{},"couchdb result object",{
				{"application/json","CouchDB result object","object","",{
								{"rows","array","Rows",{
										{"row","object","One row",{
												{"id","string","Document id - must be timestamp/10"},
												{"doc","object","Couchdb Document",{
														{"prices","assoc","List of currencies and prices",{
																{"price","number","price"}
														}}
												}}
										}}
								}}
						}}
		},{{202,"Accepted",{}}})
		.handler([&](PHttpServerRequest &req, const RequestParams &) mutable {

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
	});

	server.addPath("/symbols")
		.GET("Public","List all available symbols", "", {}, {{200,"OK",{{"application/json","symbols","assoc","List of symbols",{
				{"","string","SymbolName"},
				{"","number","Count of records"}
		}}}}})
	.handler([&](PHttpServerRequest &req, const RequestParams &params)mutable{
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
				if (k.getString() == "usd") {
					v = {0,999999,999999};
				}
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

	server.addPath("/minute")
		.GET("Public","Download minute data","",{
				{"asset","query","string","Selected asset"},
				{"currency","query","string","Selected currency"},
				{"from","query","int64","From timestamp",{}},
				{"to","query","int64","To timestamp",{},false}
		},{
				{200,"OK",{{"application/json","graph","array","Graph of prices",{
						{"pair","anyOf","",{
								{"time","int64","Time in seconds"},
								{"price","number","Price"}
						}}
				}}}}
		})
	.handler([&](PHttpServerRequest &req, const RequestParams &params){
		return generateData(pmap, req, params,1);
	});
	server.addPath("/daily")
		.GET("Public","Download daily public data","",{
				{"asset","query","string","Selected asset"},
				{"currency","query","string","Selected currency"},
				{"from","query","int64","From timestamp",{},false},
				{"to","query","int64","To timestamp",{},false}
		},{
				{200,"OK",{{"application/json","daily","array","Daily prices",{
						{"pair","oneOf","",{
								{"time","int64","Time in seconds"},
								{"price","number","Price"}
						}}
				}}}}
		})
	.handler([&](PHttpServerRequest &req, const RequestParams &params){
		return generateData(dailyPrice, req, params,daysec);
	});
	server.addPath("/ohlc")
			.GET("Public","Download OHLC public data","",{
					{"asset","query","string","Selected asset"},
					{"currency","query","string","Selected currency"},
					{"from","query","int64","From timestamp",{},false},
					{"to","query","int64","To timestamp",{},false},
					{"tfrm","query","integer","Timeframe"}
			},{
					{200,"OK",{{"application/json","ohlc","array","List of [time,o,h,l,c]",{
							{"pair","oneOf","",{
									{"time","int64","Time in seconds"},
									{"o","number","Open price"},
									{"h","number","High price"},
									{"l","number","Low price"},
									{"c","number","Close price"}
							}}
					}}}}
			})
	.handler([&](PHttpServerRequest &req, const RequestParams &params){
		if (req->getMethod() == "GET") {
			auto asset=params["asset"];
			auto currency=params["currency"];
			auto from=params["from"].getUInt();
			auto to=params["to"].getUInt();
			auto tfrm = std::max<std::size_t>(1,params["timeframe"].getUInt())*60;

			char buff[500];

			req->setContentType("application/json");;
			Stream s = req->send();
			s.putCharNB('[');
			bool comma = false;

			std::size_t lastFrame = 0;
			double o,c,h,l;

			auto flushData = [&]{
				if (lastFrame) {
					if (comma) s.write(",\n"); else comma = true;
					snprintf(buff,sizeof(buff),"[%lu, %g, %g, %g, %g]", lastFrame*tfrm, o,h,l,c);
					s.write(buff);
				}
			};

			iterateData(pmap, asset, currency, from, to, 1, [&](std::uint64_t t, double v) {
				std::size_t f = t/tfrm;
				if (f != lastFrame) {
					flushData();
					o=c=h=l=v;
					lastFrame = f;
				} else {
					c=v;
					h=std::max(h,v);
					l=std::min(l,v);
				}
			});
			flushData();
			s.putCharNB(']');
			s.flush();
			return true;
		} else {
			return false;
		}
	});
	server.addPath("/history/{time}")
		.GET("Public","Retrieve one page of the history","",{
				{"time","path","Timestamp","uint64",{}},
				{"currency","query","Specity base currency (optional)","string",{}, false}
		},{
				{200,"OK",{{"application/json","snapshot","assoc","History snapshot",{
								{"symbol","string","SymbolName"},
								{"price","number","Price"}
		}}}}})
	.handler([&](PHttpServerRequest &req, const RequestParams &params){
		if (req->getMethod() == "GET") {
			auto tm = params["~time"];
			if (!tm.defined) return false;
			json::Value at(tm.getUInt());
			bool comma = false;
			double divider = 1;
			auto cur = params["currency"];
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

	docdb::Inspector inspector(db);
	server.addPath("/inspector",[&](PHttpServerRequest &req, const std::string_view &vpath){
		if (req->getMethod()!="GET" && !checkHost(req->getHost())) {
			req->sendErrorPage(403);
			return true;
		} else {
			QueryParser qp(vpath);
			return inspector.userverRequest(req, qp);
		}
	});

	using SymbolMap=std::map<std::string, std::pair<double,unsigned int> >;

	SymbolMap symbolMap;
	std::mutex symbolMapLock;


	server.addPath("/clean")
	.POST("Admin","Remove invalid values","",{},"Request has no body",{},{{200,"OK",{}}})
	.handler([&](PHttpServerRequest &req, RequestParams vpath) {
		if (!checkHost(req->getHost())) {
			req->sendErrorPage(403);return true;
		}
		bool store = false;
		if (req->getMethod() == "POST") {
			store = true;
		}
		Batch batch;
		req->setContentType("text/plain");
		Stream s =req->send();
		json::Value symbol;
		json::Value chkTime;
		double a = 0 ,b = 0,c = 0;
		auto iter = pmap.scan();
		while (iter.next()) {
			auto key = iter.key();
			if (key[0] != symbol) {
				symbol = key[0];
				chkTime = nullptr;
				a=b=c=0;
				s.writeNB("# Checking symbol: ");
				s.writeNB(symbol.getString());
				s.writeNB("\r\n");
				s.flush();
				if (store) db.commitBatch(batch);
			}
			a = b;
			b = c;
			c = iter.value().getNumber();
			auto tm = key[1];
			if (a != 0) {
				double avgb = sqrt(a*c);
				double df1 = std::abs(avgb - b)/b;
				double df2 = std::abs(a-c)/b;
				if (df2*3 < df1 && df1>0.005) {
					char buff[1000];
					snprintf(buff,sizeof(buff),"%s %llu %g %g %g\r\n",symbol.getString().data, chkTime.getUIntLong(), a, b, c);
					s.write(buff);
					pmap.set(batch, {symbol, chkTime}, avgb);
				}
			}
			chkTime = tm;
		}
		if (store) db.commitBatch(batch);
		return true;
	});

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
					pmap.set(batch, {m.first, curTime}, m.second.first/std::max<double>(1,m.second.second));
				}
				db.commitBatch(batch);
				req->log(userver::LogLevel::progress,"Commit ", symbolMap.size(), " entries (timestamp: ",curTime,")");
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
						 symbolMap.emplace(symbol.str(), std::pair{price,0} );
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
						 if (!res.second) updatePrice(res.first->second, price);
				  }
				}
				req->setStatus(202);
				req->send("");
				return true;
			} else if (vpath == "/bitfinex") {
				for (json::Value row: body) {
					auto symbol = row[0].getString();
					if (symbol[0] == 't') {
						auto splt = symbol.substr(1).split(":");
						auto asset = splt();
						auto currency = splt();
						if (currency.empty()) {
							currency = asset.substr(3);
							asset = asset.substr(0,3);
						}
						if (currency == "USD") {
							double price = row[7].getNumber();
							std::string symbol = asset;
							std::transform(symbol.begin(), symbol.end(),symbol.begin(),[](char c){return std::tolower(c);});
							auto res = symbolMap.emplace(symbol, std::pair{price,1} );
							if (!res.second) updatePrice(res.first->second, price);
						}
					}
				}
				req->setStatus(202);
				req->send("");
				return true;
			} else if (vpath == "/binance") {
				SymbolMap smap;
				std::string symbol;
				for (json::Value row: body) {
					symbol.clear();
					auto symb = row["symbol"].getString();
					if (symb.endsWith("USDT")) {
						symbol = symb.substr(0, symb.length-4);
					} else if (symb.endsWith("BUSD")) {
						symbol = symb.substr(0, symb.length-4);
					}
					double price = row["price"].getNumber();
					if (!symbol.empty()) {
						std::transform(symbol.begin(), symbol.end(),symbol.begin(),[](char c){return std::tolower(c);});
						auto res = smap.emplace(symbol,std::pair{price,1});
						if (!res.second) updatePrice(res.first->second, price);
					}
				}
				for (const auto &row: smap) {
					double price = row.second.first/row.second.second;
					auto res = symbolMap.emplace(row.first, std::pair{price,1} );
					if (!res.second) updatePrice(res.first->second, price);
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
	server.addPath("/compact",[&](PHttpServerRequest &req, std::string_view ){
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
	server.addPath("",[&](PHttpServerRequest &req, std::string_view vpath){
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

	server.addSwagFilePath("/swagger.json");

	server.start(NetAddr::fromString(server_section.mandatory["listen"].getString(), "3456"),
			server_section.mandatory["threads"].getUInt(), 1);

	asyncProvider = server.getAsyncProvider();


	server.stopOnSignal();
	server.addThread();
	server.stop();
	logNote("---- STOP ----");





}
