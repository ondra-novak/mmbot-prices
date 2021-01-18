"use strict";

var datasrc="devel.vscgi/prices/minute.php"

var control={};
var events={
	fld_asset:["input",getInfo],
	fld_currency:["input",getInfo],
	download:["click",doDownload]
};

var available_symbols ={};

async function startApp() {

	var controls = document.querySelectorAll("[id]");
	Array.prototype.forEach.call(controls,x=>{
		control[x.id] = x;
	})
		
	for (var k in events) {
		control[k].addEventListener(events[k][0], events[k][1]);
	}

	control.fld_to.valueAsDate = new Date();
	control.fld_to.setAttribute("max", control.fld_to.value);


	var symbols =await (fetch(datasrc).then(x=>x.json()));
	control.symbols.appendChild(Object.keys(symbols).reduce((a,b)=>{
		let opt = document.createElement("option");
		opt.value = b;
		a.appendChild(opt);
		return a;
	},document.createDocumentFragment()));
		
	available_symbols = symbols;
}

function getInfo() {
	var a =  available_symbols[control.fld_asset.value] || 0;
	var b =  available_symbols[control.fld_currency.value] || 0;
	var c = a<b?a:b;
	control.download.disabled = c==0;
	control.days.innerText = (c/1440).toFixed(1);
}

function doDownload() {
	control.download.disabled = true;
	control.spinner.hidden = false;
	setTimeout(()=>{
		control.spinner.hidden = true;
	}, 5000);
	var asset = control.fld_asset.value;
	var currency = control.fld_currency.value;
	var from = (control.fld_from.valueAsDate/1000).toFixed(0);
	var to = (control.fld_to.valueAsDate/1000+24*60*60).toFixed(0);
	var url = datasrc+"?asset="+encodeURIComponent(asset)
	               +"&currency="+encodeURIComponent(currency)
	               +"&from="+from
	               +"&to="+to;
	var a = document.createElement("a");
	a.setAttribute("href", url);
	a.setAttribute("download","minute_"+asset+"_"+currency+".json");
	document.body.appendChild(a);
	a.click();
	document.body.removeChild(a);
}    