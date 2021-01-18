var dburl="../..";
"use strict";

async function start() {
	var srch = document.getElementById("search");
	srch.addEventListener("click",search);
	var repl = document.getElementById("replace");
	repl.addEventListener("click",replace);
	update_datasets();
}

async function update_datasets() {
	var data = await (fetch(dburl+"/_design/queries/_view/prices?group=true").then(x=>x.json()));
	var allsymbs = document.getElementById("allsymbs");
	while (allsymbs.firstChild) {
		 allsymbs.removeChild(allsymbs.firstChild);
	}
	data.rows.forEach(x=>{
		var opt =document.createElement("option")
		opt.value = x.key;
		allsymbs.appendChild(opt);
	});

}

async function search() {
	var symb = document.getElementById("symbol").value;
	var data = await (fetch(dburl+"/_design/queries/_view/prices?key="+encodeURIComponent(JSON.stringify(symb))).then(x=>x.json()));
	
	var res =  document.getElementById("res");
	var cnt =  data.rows.length?data.rows[0].value:0;;
	res.innerText = cnt;
	return cnt;
}

async function replace() {
	var proclabel = document.getElementById("working");
	var process_set = document.getElementById("process_set");
	var replbutt = document.getElementById("replace");
	var symb = document.getElementById("symbol").value;
	var replace = document.getElementById("repl_symbol").value;
	
	replbutt.disabled = true;
	var cnt = await search();
	var offset = 0;
	proclabel.hidden = false;
	var out = [];
	while (true) {
		process_set.innerText = offset + " / " + cnt;
		var data = await (fetch(dburl+"/_design/queries/_view/prices?reduce=false&include_docs=true&key="+encodeURIComponent(JSON.stringify(symb))+"&skip="+offset+"&limit=100").then(x=>x.json()));
		if (data.rows.length == 0) break;
		var docs = data.rows.map(x=>x.doc);
		offset+=100;		
		docs = docs.map(x=>{
			if (replace && x.prices[replace]) {
				return null;
			} else {
				if (replace) { 
					x.prices[replace] = x.prices[symb];
				}
				delete x.prices[symb];
			}
			return x;
		}).filter(x=>(x!==null));
		if (docs.length) {
			out.push(docs);		
		}
	}
	process_set.innerText = "Uploading...";
	var promises = out.map(batch=>{
		return fetch(dburl+"/_bulk_docs",{
			method:"POST",
			headers:{
				"Content-Type":"application/json"
			},
			body:JSON.stringify({docs:batch})
			});
	})
	try {
		await Promise.all(promises);	
	} catch (e) {
		console.error(e);
	}
	replbutt.disabled = false;
	proclabel.hidden = true;
	update_datasets();
	search();	
}
