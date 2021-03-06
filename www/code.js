"use strict";

var datasrc="minute";
var symbols_src="symbols";

var control={};
var events={
	fld_asset:["input",getInfo],
	fld_currency:["input",getInfo],
	fld_from:["input",getInfo],
	fld_to:["input",getInfo],
	download:["click",doDownload]
};

var available_symbols ={};
var stockChart;

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


	var symbols =await (fetch(symbols_src).then(x=>x.json()));
	control.symbols.appendChild(Object.keys(symbols).reduce((a,b)=>{
		let opt = document.createElement("option");
		opt.value = b;
		a.appendChild(opt);
		return a;
	},document.createDocumentFragment()));
		
	available_symbols = symbols;
	stockChart = new CanvasJS.Chart("chartContainer",{
				    theme: "light2",
				    backgroundColor: "#CFCFCF",
				    zoomEnable: true,
			    
			      axisX: {
			        labelAngle: -45
			      },
			      axisY: {
    			   	includeZero:false
				  },
				  data: [{
				  	   color: "#001020",
			          type: "candlestick",			          			          
		               dataPoints : []
			          }]			      
          });
}

function getInfo() {
	var a =  available_symbols[control.fld_asset.value] || [0, 0, 0];
	var b =  available_symbols[control.fld_currency.value] || [0,0,0];
	var beg = a[0]>b[0]?a[0]:b[0];
	var end = a[1]<b[1]?a[1]:b[1];
	var days = end - beg + 1;
	if (days<0 || end == 0) days = 0
	control.download.disabled = days == 0;
	control.days.innerText = days;
	var from_tm = beg*24*60*60*1000;
	var to_tm = end*24*60*60*1000;
	var begdate = new Date(from_tm);
	var enddate = new Date(to_tm);
	var datemin = begdate.toISOString().substr(0,10);
	var datemax = enddate.toISOString().substr(0,10);
	if (days) {
		control.fld_to.setAttribute("min", datemin); 
		control.fld_to.setAttribute("max", datemax); 
		control.fld_from.setAttribute("min", datemin); 
		control.fld_from.setAttribute("max", datemax); 
		if (control.fld_from.value<datemin) control.fld_from.value = datemin;
		if (control.fld_from.value>datemax) control.fld_from.value = datemax;
		if (control.fld_from.value>control.fld_to.value)
		                        control.fld_to.value = datemax;
		if (control.fld_to.value<datemin) control.fld_to.value = datemin;		
		if (control.fld_to.value>datemax) control.fld_to.value = datemax;
	}
	updateChart(control.fld_asset.value, control.fld_currency.value, 
        fld_from.valueAsDate, fld_to.valueAsDate
	);
		
}




async function updateChart(asset, currency, from, to) {
    var from_tm = Math.floor(from/1000);
    var to_tm =  Math.floor(to/1000)+86400;
    var dist = Math.abs(to_tm - from_tm)/86400;
    var frame;
    if (dist < 4) frame = 15;
    else if (dist < 15) frame = 60;
    else if (dist < 60) frame = 240;
    else frame = 1440;

	var data = await fetch("ohlc?asset="+encodeURIComponent(asset)
			+"&currency="+encodeURIComponent(currency)
			+"&from="+from_tm
			+"&to="+to_tm
			+"&timeframe="+frame).then(x=>x.json());
	
	if (data.length) {		
		var opts =  stockChart.options;
		opts.data[0].dataPoints = data.map(v=>{
			var x = new Date(v[0]*1000);
			var y = [v[1],v[2],v[3],v[4]];
			return {x:x,y:y};	
		});
		stockChart.render();	
	}
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