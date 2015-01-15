var util = require('util');
var path = require('path');
var https = require('https');
var fs = require('fs');
var spawn = require('child_process').spawn;
var MSPERDAY = 86400000;
var DAY_REF = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
var l = console.log;

var apiKey = "key-5cabb41e0033368d80d7e8583e610718";
var mailgun = require('mailgun-js')({ apiKey: apiKey , domain: 'spage.us' });


Date.prototype.formatSql = function() {
	var m = this.getMonth() + 1;
	if (m.toString().length == 1) m = "0" + m;
	var d = this.getDate();
	if (d.toString().length == 1) d = "0" + d;
	var str  = this.getFullYear()+"-" + m + "-"+ d;
	return str;
}
Date.prototype.formatMdy = function() {
	var m = this.getMonth() + 1;
	if (m.toString().length == 1) m = "0" + m;
	var d = this.getDate();
	if (d.toString().length == 1) d = "0" + d;
	var str  = m + "-" + d + "-" + this.getFullYear();
	return str;
}

module.exports = function(config){

	var self = this;
	this.version = 1.0;
	this.config = config;
	this.MSPERDAY = MSPERDAY;
	this.data = {};


	this.remove_nulls_from_arr = function( arr ){
		var a=[];
		for(var i = 0; i < arr.length; i++) if ( arr[i]!==null ) a.push(arr[i]);
		return a;
	}
	this.unique = function( arr ){
		var a=[];
		for(var i = 0; i < arr.length; i++) if ( arr.indexOf(arr[i])!==-1 ) a.push(arr[i]);
		return a;
	}

	this.sort_by_ts = function(a,b){
		if ( a.ts > b.ts ) return 1; else return -1;
	}
	this.json_log = function ( pkg , path){
		var str =  JSON.stringify( pkg, null, "\t" );
		if ( undefined !== path ){
			l("logging to " + path);
			fs.writeFileSync( path, str);
		}
		else l( str );
	}

	this.split_txt = function ( txt, size ){
		if ( txt.length <= size  ) return [ txt ];

		var cn = Math.floor( txt.length / size );
		var arr = [];
		for(var i = 0 ; i < cn ; i++){
			var pos = i * size;
			arr.push( txt.substr( pos, size ) );
		}
		var remain =  ( txt.length % size );
		if ( remain ) arr.push( txt.substr( -1 * remain ) );

		return arr;
	}

	this.round = function(flt, places){
		if (places==undefined) places = 5;
		var f = Math.pow(10,places);
		return Math.round( flt * f ) / f;
	}

	this.usd = function(flt){
		var t=(Math.round(flt*100)/100).toFixed(2);
		var a=t.split(".");
		var w=a[0];
		if(Math.ceil(w.length/3)<=1) return t;
		var txt="."+a[1]; var x=false;
		for(var i=0; i<=(Math.ceil(w.length/3)-1); i++){
			if(i>0) txt=","+txt;
			x=w.length-(3*(i+1));
			if(x>=0) txt=w.substr( x,3 )+txt;
			else{
				if(x==-1) txt=w.substr( 0,2 ) + txt;
				else txt=w.substr( 0,1 ) + txt;
			}
		}
		if (txt.substr(0,2)=="-,") txt="-"+txt.substr(2);
		return txt;
	};

	this.round_to_string = function(flt, places){
		if (places==undefined) places = 5;
		var f = Math.pow(10,places);
		var n = ( Math.round( flt * f ) / f ).toString();
		var idx = n.search(/\./g);
		if ( idx == -1 )
			return n+".00";

		if ( n.substr(-2,1) == "." ) 
			return n+"0";
		
		return n;
	}

	this.pad_front = function(str){
		var a = [];
	    var n = 16 - str.length; 
		for(var i = 0; i < n; i++) a.push("0"); 
		return a.join("")+str;
	}





	this.getWeekPeriodByEndDate = function( endDt ){
		var ed = (new Date(endDt)).getTime();
		var sd = new Date( ed - (MSPERDAY * 6) );
		sd = sd.formatSql();
		ed = endDt.formatSql();
		return {startDt: sd, endDt: ed };	
	}

	this.getMonthYearIndexForLastMonth = function(){
		var target_yr = (new Date()).getFullYear();
		var target_month = (new Date()).getMonth()-1;
		if (target_month==-1){ 
			target_month==11;
			target_yr--;
		}
		return {yr: target_yr, month: target_month};
	}


	this.zip = function(cb){
		var zip_file_name = self.config.end_dir.toLowerCase() + ".zip" ;
		if ( self.files_to_zip.length == 0 ) return cb(null,zip_file_name); 
		var fn = self.files_to_zip.pop();
		var psTxt="", psErr="";
		var ps_p = spawn(
			'/usr/bin/zip', 
			[ 
				zip_file_name,
				self.config.end_dir+"/"+fn
			],
			{ cwd: self.config.data_path } 
		);
		ps_p.stdout.on("data",function(data){ psTxt+=data.toString(); });
		ps_p.stderr.on("data",function(data){ psErr+=data.toString(); });
		ps_p.on("exit",self.zip);
	}

	this.transfer = function(fn,url_to_download_from,cb){
		/*
		linux command:  scp a.txt root@catfiles.twinmed-dev.com:"/tmp"
		var secret_path = "8293XMNP-89-VN3022-1001";
		var remote_path = 'root@catfiles.twinmed-dev.com:"/var/www/catfiles/INVOICES/'+secret_path+'"';

		Example:
		url_to_download_from  = "http://catfiles.twinmed-dev.com/INVOICES/"+secret_path + "/"+fn;
		*/
		var ps_p = spawn(
			'/usr/bin/scp', 
			[ fn , self.config.remote_path ] , 
			{ cwd: self.config.data_path }  
		);
		var psTxt="", psErr="";
		ps_p.stdout.on("data",function(data){ psTxt+=data.toString(); });
		ps_p.stderr.on("data",function(data){ psErr+=data.toString(); });
		ps_p.on("exit",function(){
			l(psTxt);
			var url = url_to_download_from + "/" + fn;
			cb(null,url);
		});
	}

	/*
		ssh -i ~/.ssh/pems/tmsmallubuntu.pem ubuntu@50.18.116.8 'cd /mnt/www/catfiles/RAMPDF/x329sd2302; /bin/tar vxfz /mnt/www/catfiles/RAMPDF/x329sd2302/1370971460942-42.tar.gz'
	*/
	this.remote_comm_pem = function(pem,host,comm,cb){
		var ps_p = spawn(
			'/usr/bin/ssh', 
			['-i', pem, host, comm ]
		);
		var psTxt="", psErr="";
		ps_p.stdout.on("data",function(data){ psTxt+=data.toString(); });
		ps_p.stderr.on("data",function(data){ psErr+=data.toString(); });
		ps_p.on("exit",function(){
			if ( psErr ) return cb(psErr);
			cb(null,"completed command " + comm + ":" +psTxt);
		});
	}


	//execute php -f , return result as parsed json
	this.php = function(fn, php_args, cb){
		php_args = ["-f", fn].concat(php_args);	
		var ps_p = spawn('php', php_args );
		var psTxt="";
		var psErr="";

		ps_p.stdout.on("data",function(data){ psTxt+=data.toString(); });
		ps_p.stderr.on("data",function(data){ psErr+=data.toString(); });
		ps_p.on("exit",function(code){
			if (code==0 || psErr!="" ){
				//error with sql
				l('php errror',code,psTxt,psErr);
				return cb({err:psErr},false);
			}
			psTxt = psTxt.trim();
			try {
				if (!psTxt) var pkg={status:false};
				else var pkg= JSON.parse(psTxt);
			}
			catch(err){
				l("Could not parse result from ms query", err, psTxt);
				return cb({err:err,psTxt:psTxt},false);
			}
			return cb(null,pkg);
		});
	}

	this.mysql = function(sql, opts, cb){
		if ( typeof sql == "object" && sql.hasOwnProperty("length") ) sql = sql.join("\n");
		var ps_p = spawn('php', ["-f", "mysql_for_node.php", "v2",  sql], { cwd: "/var/www/csweb/lc" } );
		self.tsql( ps_p, sql, opts, cb);
	}

	//filebound
	this.fb_sql = function(sql, opts, cb){
		if ( typeof sql == "object" && sql.hasOwnProperty("length") ) sql = sql.join("\n");
		var ps_p = spawn('php', ["-f", "phpsql_for_node.php", "fb",  sql], { cwd: "./phplib" } );
		self.tsql( ps_p, sql, opts, cb);
	}


	this.fb6_sql = function(sql, opts, cb){
		if ( typeof sql == "object" && sql.hasOwnProperty("length") ) sql = sql.join("\n");
		var ps_p = spawn('php', ["-f", "phpsql_for_node.php", "fb6",  sql], { cwd: "./phplib" } );
		self.tsql( ps_p, sql, opts, cb);
	}

	this.hdms_prod_sql = function(sql, opts, cb){
		if ( typeof sql == "object" && sql.hasOwnProperty("length") ) sql = sql.join("\n");
		var ps_p = spawn('php', ["-f", "phpsql_for_node.php", "hdms_prod",  sql], { cwd: "./phplib" } );
		self.tsql( ps_p, sql, opts, cb);
	}

	this.hdms_sql = function(sql, opts, cb){
		if ( typeof sql == "object" && sql.hasOwnProperty("length") ) sql = sql.join("\n");
		var ps_p = spawn('php', ["-f", "phpsql_for_node.php", "hdms_replica",  sql], { cwd: "./phplib" } );
		self.tsql( ps_p, sql, opts, cb);
	}

	this.hdms_sp_sql = function(sql, opts, cb){
		if ( typeof sql == "object" && sql.hasOwnProperty("length") ) sql = sql.join("\n");
		var ps_p = spawn('php', ["-f", "phpsql_for_node.php", "hdms_replica_sp",  sql], { cwd: "./phplib" } );
		self.tsql( ps_p, sql, opts, cb);
	}


	this.v2sql = function(sql, opts, cb){
		if ( typeof sql == "object" && sql.hasOwnProperty("length") ) sql = sql.join("\n");
		var ps_p = spawn('php', ["-f", "mysql_for_node.php", "v2",  sql], { cwd: "/var/www/csweb/lc" } );
		self.tsql( ps_p, sql, opts, cb);
	}



	this.tsql = function(ps_p, sql, opts, cb){
		if (typeof opts=="function"){
			cb = opts;
		}
		else if (opts==undefined){  //no callback
			cb = function(a,b){ l(a,b); }
		}

		var psTxt="";
		var psErr="";
		ps_p.stdout.on("data",function(data){ 
			//l(data.toString());
			psTxt+=data.toString(); 
		});

		ps_p.stderr.on("data",function(data){ 
			//l("er:",data.toString());
			psErr+=data.toString(); 
		});
		ps_p.on("exit",function(code){
			if (code==0 || psErr!="" ){
				var conn_err = "Warning: mssql_connect()";
				var is_conn_err=false;
				if ( psTxt.substr(1, conn_err.length) == conn_err ){
					l("Got mssql connect error");
					is_conn_err=true;
				}
				return cb({ 
					php_exit_code: code,
					psTxt: psTxt,
					psErr: psErr,
					is_conn_err: is_conn_err,
					sql: sql.replace(/\n/g," ")
				});
			}

			psTxt = psTxt.trim();
			try {
				if (!psTxt) var rr=[];
				else var rr = JSON.parse(psTxt);
			}
			catch(err){
				l("Could not parse result from ms query", err, psTxt);
				return cb(err,false);
			}


			if (typeof opts=="function"){
				return opts(null,rr); //opts is really the callback
			}

			return self.iterate_options( opts, rr, cb );

		});
	}

	this.iterate_options = function( opts, rr, cb  ){

		opts.to_float  	= opts.to_float || [];
		opts.to_int  	= opts.to_int || [];
		opts.to_fn  	= opts.to_fn || {fields:[]};
		opts.to_arr  	= opts.to_arr || null;

		for(var i=0; i < rr.length; i++){
			rr[i] = self.to_float(rr[i],opts.to_float);
			rr[i] = self.to_int(rr[i],opts.to_int);
			rr[i] = self.to_fn(rr[i],opts.to_fn.fields, opts.to_fn.fn);
			if ( opts.to_arr )
				rr[i] = rr[i][ opts.to_arr ];
		}

		/*
		if ( opts.to_arr ) {
			for(var i=0; i < rr.length; i++) rr[i] = rr[i][ opts.to_arr ]; 
			if ( cb ) return cb(null,rr);
			return;
		}

		if ( opts.to_keys ){
			var kk = {};
			if ( typeof opts.to_keys.v == "string" )
				for(var i=0; i < rr.length; i++) kk[rr[i][opts.to_keys.k]] = rr[i][ opts.to_keys.v ];
			else if ( typeof opts.to_keys.v == "object" )
				for(var i=0; i < rr.length; i++) kk[rr[i][opts.to_keys.k]] = opts.to_keys.v;
			else
				for(var i=0; i < rr.length; i++) kk[rr[i][ opts.to_keys.k ]] = rr[i];
				
			if ( cb ) return cb(null,{ n: rr.length, rr: kk});
			return;
		}


		var it_fns = [];  //fn list to iterate through
		if ( opts.to_float ) it_fns.push( [ self.to_float, opts.to_float, null ] );
		if ( opts.to_int ) it_fns.push( [ self.to_int, opts.to_int, null ] );
		if ( opts.to_fn ) it_fns.push( [ self.to_fn, opts.to_fn.fields, opts.to_fn.fn  ] );

		for(var i=0; i < rr.length; i++){
			for(var j = 0 ; j < it_fns.length; j++){
				//l( typeof (  it_fns[ j ][0] ) );
				rr[i] = it_fns[ j ][0](   rr[i], it_fns[j][1], it_fns[j][2] );
			}
		}
		*/
		if ( cb ) return cb(null,rr);
		return;
	}


	this.scp = function( from, to, cb){
		var psTxt="";
		var ps_p = spawn('scp', [from, to], { cwd: "/var/www/csweb/lc" } );
		ps_p.stdout.on("data",function(data){ 
			psTxt+=data.toString(); 
		});

		ps_p.stderr.on("data",function(data){ 
			psTxt+=data.toString(); 
		});
		ps_p.on("exit",function(code){
			if (code!=0){
				//error with sql
				l('scp errror');
				l(psTxt);
				return cb(psTxt);
			}

			return cb(null);
		});
	}

	this.scp_new  = function( from, to, cb){
		var psTxt="";
		var ps_p = spawn('scp', [from, to] );
		ps_p.stdout.on("data",function(data){ 
			psTxt+=data.toString(); 
		});

		ps_p.stderr.on("data",function(data){ 
			psTxt+=data.toString(); 
		});
		ps_p.on("exit",function(code){
			if (code!=0){
				//error with sql
				l('scp errror');
				l(psTxt);
				return cb(psTxt);
			}

			return cb(null);
		});
	}
	this.scp_pem = function(from,to,pem,cb){
		l("\nAttempting scp " + from + " to " + to + "\n");
		var ps_p = spawn(
			'/usr/bin/scp', 
			['-i', pem, from, to ]
		);
		var psTxt="", psErr="";
		ps_p.stdout.on("data",function(data){ psTxt+=data.toString(); });
		ps_p.stderr.on("data",function(data){ psErr+=data.toString(); });
		ps_p.on("exit",function(){
			if ( psErr ) return cb(psErr);
			cb(null,psTxt,"completed scp " + from + " to " + to);
		});
	}

	this.to_float = function(row, kk){
		for(var i=0; i <  kk.length; i++){
			var k = kk[i];	
			if (row[k]){
				//row[k] = self.round( parseFloat( row[k].replace(/[^0-9-\.]/g,"" ) ), 2 );
				row[k] = self.round( parseFloat( row[k].replace(/[^0-9-\.]/g,"" ) ), 5 );
			}
		}
		return row;
	}

	this.to_int = function(row, kk){
		for(var i=0; i <  kk.length; i++){
			var k = kk[i];	
			if (row[k]){
				row[k] = self.round( parseInt( row[k] ,10), 0 );
			}
		}
		return row;
	}

	this.to_fn = function(row, kk, fn){
		for(var i=0; i <  kk.length; i++){
			var k = kk[i];	
			if (row[k]){
				row[k] = fn( row[k] );
			}
		}
		return row;
	}


	this._get_period_data_by_last_day  = function( period, last_day_str ){
		switch( period ){ 
			case "weekending":
			case "weekly":
				var days_to_minus = 6;
				var period_factor = {
					"weekly": 1,
					"bi-weekly": .5 
				}
				break;
			case "2weeksending":
			case "bi-weekly":
				var days_to_minus = 13;
				var period_factor = {
					"weekly": 2,
					"bi-weekly": 1
				}
				break;
		}

		// end_dt is LAST DAY IN PERIOD INCLUSIVE
		var end_dt =  self.get_month_day_from_str( last_day_str );
		if ( false === end_dt ){
			end_dt = new Date();
		}
		end_dt = new Date( self.get_top_of_day( end_dt ) );
		var ms_end_dt = end_dt.getTime(); 
		var ms_start_dt = ms_end_dt - (self.MSPERDAY * days_to_minus);

		var start_dt = new Date( self.get_top_of_day( ms_start_dt ) );
		var day_after_end_dt = new Date( self.get_top_of_day( ms_end_dt + self.MSPERDAY ) );

		var period_display_txt = [
			start_dt.toString().substr(0,15) ,
			" to ",
			end_dt.toString().substr(0,15) 
		].join("");

		return {
			period: period,
			last_day_str: last_day_str,
			period_factor: period_factor,
			start_dt: start_dt,
			end_dt: end_dt,
			day_after_end_dt: day_after_end_dt,
			period_display_txt: period_display_txt
		};

	}



	this.get_top_of_day = function(dt){
	l(dt);
		if ( typeof dt == "number" ) dt = new Date(dt);
		dt=new Date(self.get_top_of_hour(dt));
		return (dt.getTime()-(60*60*1000*dt.getHours()));
	}

	this.get_top_of_hour = function(dt){
		l(dt);
		if ( typeof dt == "number" ) dt = new Date(dt);
		return (dt.getTime() - ( 60*1000*dt.getMinutes() + 1000*dt.getSeconds() + dt.getMilliseconds() ));
	}

	this.min_to_hrs_and_min = function(min){
		return [ Math.floor( min / 60 ), " hour(s) and ", ( min % 60 ) , " minutes" ].join("");
	}


	this.get_minutes_from_str = function(entry){
		var val = parseInt(entry,10);
		if ( true == isNaN( val ) ){ 
			return false;	
		}


		if ( entry.substr(-1) == "m" ) {
			// minutes only	
			return val;
		}
		else switch( entry.length ){

			case 1: 	// typed in a single hour, 0 minutes  (example:  3	for 3 hours )
			case 2: 	// typed 2 double digit hours, 0 minutes ( example: 10  for 10 hours )
				if ( val <= 8 ) //assume hours
					return ( 60 * val );

				return val; //assume minutes

			case 3: 	// typed single digit hour and minutes ( example: 325 	for 3 hours, 25 minutes )
				return ( parseInt( entry[0] ) * 60 ) + parseInt( entry.substr(1) );

			default: // typed dbouble digit hours and minutes ( example: 1015 for 10 hours, 15 minutes )
				return ( parseInt( entry.substr(0,2) ) * 60 ) + parseInt( entry.substr(2) );

		}
	}

	this.get_month_day_from_str = function(entry, year){  // acceptable: 6-13 6.13 6,23 
		var now = new Date();
		if ( entry == "" || entry == "0" || entry == "today" ) return now;

		switch( typeof year == 'object' ){
			case 'object':
				now = year; //2nd argument is assumed to be a date object
				year = now.getFullYear();
				break;
			case 'number':
				year = parseInt(year);
				var now = new Date(year, now.getMonth(), getDate());
				break;
			default:	
				year = (new Date()).getFullYear();
				break;
		}

		switch( entry.length ){
			case 1:
				return false;

			case 2:
				if ( entry[0] == "-" || entry[0] == "+") { //user is indicating a date offset
					if ( isNaN( entry[1] ) )
						return false;

					var days_offset = parseInt( entry[1] );
					l(days_offset , "days_offset ");
					if ( entry[0] == "-" )
						return new Date( now.getTime() - (days_offset * MSPERDAY) );

					return new Date( now.getTime() + (days_offset * MSPERDAY) );
				}
			default: //theres 3 or more characters

				var rg = /[0-9]{1,2}[^0-9][0-9]{1,2}/g;
				var match_result = entry.match(rg);
				if ( !match_result ){
					return false;	
				}

				var parts = match_result[0].split(/[^0-9]/g);
				if ( parts.length !== 2 ){
					return false;
				}
				else{
					var m = parseInt(parts[0]);					
					var d = parseInt(parts[1]);					
l(m,d);
					var mr = [0,31,28,31,30,31,30,31,31,30,31,30,31];
					if ( 0 === (year % 4) ) mr[1]=29;

					if ( m > 12 ) return false;	
					if ( d > mr[m] ) return false; 

					return new Date( year, m-1, d );
				}
				break;
		}
		return false;
	}



	/*
		Jan 15, 2000 would be month: 0
		Feb 1, 2004 would be month:  ( 4 * 12 ) + 1 = 49
	*/
	this.get_month_idx_in_century = function(dt){
		return ( dt.getYear() - 100 ) * 12 +  dt.getMonth();
	}

	this.dt_to_str = function(dt, fmt){
		var r = {y:0,m:1,d:2,h:3,n:4,s:5};
		var pp = self.dt_parts(dt);
		return [ pp[r.m], pp[r.d], pp[r.y]].join("-");
	}

	this.dt_to_local_dt_time = function(dt, as_array){
		//localize
		//dt = new Date(  dt.getTime() - ( dt.getTimezoneOffset()*60*1000) );
		//return dt.toString().substr(0,24);
		var r = {y:0,m:1,d:2,h:3,n:4,s:5};
		var pp = self.dt_parts(dt, true);
		var mer = "AM";
		if ( pp[r.h] > 11 ){ 
			mer = "PM";
		}
		if ( pp[r.h] > 12 ){ 
			pp[r.h] = pp[r.h]-12;
		}
		else if ( pp[r.h] < 1 ){ 
			pp[r.h] = 12;
		}
		var day = DAY_REF[ dt.getDay() ];
		if ( as_array === true )
			return [day , [ pp[r.m], pp[r.d], pp[r.y]].join("-") , [ pp[r.h], pp[r.n]].join(":") + " " + mer ];

		return [day , [ pp[r.m], pp[r.d], pp[r.y]].join("-") , [ pp[r.h], pp[r.n]].join(":") + " " + mer ].join(" ");
	}

	this.dt_to_yyyymmdd = function(dt){
		if ( typeof dt == "number" ) dt = new Date(dt);
		var fy = dt.getFullYear();
		var m = (1+dt.getMonth()); if (m< 10) m = "0"+m; else m = m.toString();
		var d = dt.getDate(); if (d< 10) d = "0"+d; else d = d.toString();
		return fy+"-"+m+"-"+d;
	}

	this.dt_to_str_no_space = function(dt){
		if ( typeof dt == "number" ) dt = new Date(dt);
		var fy = dt.getFullYear();
		var m = (1+dt.getMonth()); if (m< 10) m = "0"+m; else m = m.toString();
		var d = dt.getDate(); if (d< 10) d = "0"+d; else d = d.toString();
		return fy+m+d;
	}


	this.dt_parts = function(dt,as_ints){
		if ( typeof dt == "number" ) dt = new Date(dt);
		var fy = dt.getFullYear();
		if ( as_ints === true ){  //except for minutes
			var m = (1+dt.getMonth()); 
			var d = dt.getDate(); ;
			//var h = dt.getUTCHours(); 
			var h = dt.getHours(); 
			var min = dt.getMinutes(); if (min< 10) min = "0"+min; else min = min.toString();
			var sec = dt.getSeconds(); 

			return [ fy,m,d,h,min,sec ];
		}

		var m = (1+dt.getMonth()); if (m< 10) m = "0"+m; else m = m.toString();
		var d = dt.getDate(); if (d< 10) d = "0"+d; else d = d.toString();
		//var h = dt.getUTCHours(); if (h< 10) h = "0"+h; else h = h.toString();
		var h = dt.getHours(); if (h< 10) h = "0"+h; else h = h.toString();
		var min = dt.getMinutes(); if (min< 10) min = "0"+min; else min = min.toString();
		var sec = dt.getSeconds(); if (sec< 10) sec = "0"+sec; else sec = sec.toString();

		return [ fy,m,d,h,min,sec ];
	}

	this.dt_time_to_str_no_space = function(dt){
		if ( typeof dt == "number" ) dt = new Date(dt);
		var arr_of_parts = self.dt_parts(dt);
		return arr_of_parts.join("");
	}

	this.dt_time_to_utc_str = function(dt){
		// format:  2007-08-30T10:20:34Z	
		if ( typeof dt == "number" ) dt = new Date(dt);
		var arr_of_parts = self.dt_parts(dt);

		return arr_of_parts.splice(0,3).join("-") + "T" + arr_of_parts.slice(0,3).join(":") + "Z";
	}


	this.str_to_date = function(dtstr){ //yyyy-mm-dd
		var y = parseInt( dtstr.substr(0,4), 10 );
		var m = parseInt( dtstr.substr(5,2), 10 );
		var d = parseInt( dtstr.substr(8,2), 10 );

		if ( isNaN(y) || isNaN(m) || isNaN(d) ) return false;
		m--;
		return new Date(y,m,d);
	}

	this.to_mdy = function(dtstr){ //yyyy-mm-dd  to mm-dd-yyyy
		return [ dtstr.substr(4,2), dtstr.substr(6,2), dtstr.substr(0,4) ].join("-");
	}

	this.mm_dd_yyyy_to_yyyymmdd = function(dtstr){ //mm-dd-yyyy to  yyyy-mm-dd 
		var pts = dtstr.split(/[\/\-\.]/g);
		return [ pts[2], pts[0], pts[1] ].join("");
	}


	this.mm_dd_yyyy_to_dt = function(dtstr){ //mm-dd-yyyy to Date object
		var pts = dtstr.split(/[\/\-\.]/g);
		return new Date( pts[2], parseInt(pts[0],10)-1, pts[1] );
	}

	//make best guess on what the date string is stating
	this.to_dt = function(dtstr, if_not_dt, mill){ //mm-dd-yyyy to Date object
		if ( if_not_dt === undefined ) if_not_dt=false;
		dtstr=dtstr.trim().replace(/ {2,}/," ").replace(/ /g,"-");

		if ( dtstr.search(/^[janfebmrpyulgsoctvd]{3}/i) === 0 ){
			var m_txt = dtstr.substr(0,3).toLowerCase();
			var m_pos = [ false,"jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"].indexOf(m_txt);
			dtstr = dtstr.replace(/^[janfebmrpyulgsoctvd]{3}/i , m_pos )
		}
		
		if ( dtstr.search( /[0-9]{1,2}[^0-9][0-9]{1,2}[^0-9][0-9]{2}|[0-9]{4}/ ) !== 0 )
			return if_not_dt;

		var pts = dtstr.split(/[^0-9]/g);
		var m = parseInt( pts[0], 10 );
		var d = parseInt( pts[1], 10 );
		var y = parseInt( pts[2], 10 );
		if ( y < 100 && mill!==undefined){
			y += mill;	
		}

		if ( isNaN(y) || isNaN(m) || isNaN(d) ) return if_not_dt;
		if ( m>12 || d>31 ) return if_not_dt;

		m--;
		return new Date(y,m,d);
	}

	this.add_dashes = function(dtstr){ //201406080815 
		return [dtstr.substr(0,4),dtstr.substr(4,2),dtstr.substr(6)].join("-");
	}

	this.yyyymmddhhmm_to_date = function(dtstr){ //201406080815 

		var y = parseInt( dtstr.substr(0,4), 10 );
		var m = parseInt( dtstr.substr(4,2), 10 );
		var d = parseInt( dtstr.substr(6,2), 10 );
		var hh = parseInt( dtstr.substr(8,2), 10 );
		var mm = parseInt( dtstr.substr(10,2), 10 );

		if ( isNaN(y) || isNaN(m) || isNaN(d) ) return false;
		m--;
		return new Date(y,m,d,hh,mm);
	}

	this.yyyymmdd_to_date = function(dtstr){ //201406080815 

		var y = parseInt( dtstr.substr(0,4), 10 );
		var m = parseInt( dtstr.substr(4,2), 10 );
		var d = parseInt( dtstr.substr(6,2), 10 );

		if ( isNaN(y) || isNaN(m) || isNaN(d) ) return false;
		m--;
		return new Date(y,m,d);
	}


	this.sort_by = function(arr, k){
		arr.sort(function(a,b){
			if ( a[k] > b[k] ) return 1; else if ( a[k] < b[k] ) return -1; return 0;
		});
		return arr;
	}


	this.days_apart = function(a,b){
		if (typeof a == "string" ) a = self.str_to_date(a);
		if (typeof b == "string" ) b = self.str_to_date(b);
		var d = Math.ceil( ( b.getTime()  - a.getTime() ) / MSPERDAY ) + 1;
		//if ( d < 0 ) d *= -1;
		return d;
	}


	this.calc_age = function(dt1, dt2){ //yyyy-mm-dd
		//default to statement in days
		var stack = [ 0, 0 , 0, 0 ];  // [ d, h, m , 0]

		var seconds  = Math.round( ( dt2.getTime()  - dt1.getTime()) / 1000  ); 
		if ( seconds < 60 ){
			stack[3]=seconds;
			return stack;
		}
		var minutes = Math.floor( seconds / 60 );  // a is now expressed  in minutes
		stack[3] = seconds % 60;  //seconds left over
		if ( minutes < 60 ){
			stack[2] = minutes;
			return stack;
		}
		var hours = Math.floor( minutes / 60 );  // a is now in hours
		stack[2] = minutes % 60; //minutes left over

		if ( hours < 24 ){
			stack[1] = hours;
			return stack;
		}
		stack[1] = hours % 24;
		stack[0] = Math.floor( hours / 24 ); 	
		return stack;
	}

	this.calc_age_str = function(dt1, dt2, suffix, suffix_if_zero){
		var stack = self.calc_age( dt1, dt2 );
		var str = [];
		if ( stack[0] > 0 ){ 
			if ( stack[0] > 1 ) str.push( stack[0] + " Days");
			else str.push( stack[0] + " Day");
		}

		if ( stack[1] > 0 ){
			if ( stack[1] > 1 ) str.push( stack[1] + " Hrs");
			else str.push( stack[1] + " Hr");
		}

		if ( stack[2] > 0 )
			str.push( stack[2] + " Min");

		if ( str.length == 0 ) return suffix_if_zero;
		return str.join(", ") + " " + suffix;
	}


	this.enter = function(parent_ref, key, ob ){
		if ( undefined == parent_ref[ key ] ){
			parent_ref[key] = ob;
		}
		return parent_ref[key];
	}

	this.add_to_list = function(parent_ref, item){
		if ( parent_ref.indexOf(item)==-1 ){
			parent_ref.push(item);
		}
		return parent_ref;
	}

	this.pp = function(p,d,cb){
		//l(prettyjson.render(d));
		if ( cb == undefined ) cb = function(){};
		if ( d.hasOwnProperty("length") ){
			var stack = [];
			for(var i=0; i < d.length; i++){
				var o = {};
				if ( typeof d[i] == "object" )
					for(var k in d[i]){
						if ( typeof d[i][k] !== "function" ) o[k]=d[k];
					}
				stack.push(o);
			}
			return fs.writeFile(p,JSON.stringify(stack),cb);
		}

		var o = {};
		for(var k in d){
			if ( typeof d[k] !== "function" ) o[k]=d[k];
		}
		return fs.writeFile(p,JSON.stringify(o),function(){
			setTimeout(function(){
				var args = ["-mjson.tool",p];
				var ps_p = spawn('/usr/bin/python', args );
				var psTxt="";
				var psErr="";
				ps_p.stdout.on("data",function(data){ psTxt+=data.toString(); });
				ps_p.stderr.on("data",function(data){ psErr+=data.toString(); });
				ps_p.on("exit",function(){
					fs.writeFile(p+".js",  psTxt  ,  cb);
				});
			},2000);
		});
	};


	this.tabs_to_array = function(fn,opts,cb){
		self.read(fn,function(err,data){
			if ( err ) {
				l("Could not read file " + fn);
				return cb(err);
			}
			var rows = data.toString().split(/\n/);
			//l("Found " + rows.length);

			var cols = rows[0].split(/\t/);
			for(var i = 0; i < cols.length; i++){
				cols[i] = 	cols[i].toLowerCase()
							.replace(/^ *| *$/g,"")
							.replace(/ {2,}/g," ")
							.replace(/[^a-z0-9-]/g,"_");
			}
			//l(cols);

			var arr =[];
			for(var i = 1; i < rows.length; i++){
				var ob = {};
				var row = rows[i].split(/\t/);
				for(var j = 0; j < cols.length; j++){
					ob[ cols[j] ] = row[j];
				}
				arr.push(ob);
			}

			if ( !arr.length ) return cb(null,[]);


			var pop_last=true;
			for(var j = 0; j < cols.length; j++){
				if ( undefined !== ob[ cols[j]] && '' !== ob[ cols[j] ] ) 
					pop_last=false;
			}

			if ( pop_last ) arr.pop();

			if (typeof opts=="function"){
				return opts(null,arr); //opts is really the callback
			}

			return self.iterate_options( opts, arr, cb );
		});
	}

	this.array_of_obs_to_xls = function(arr, add_col){
		var rows =[];
		if (!arr.length) return [];

		var cols = [];
		for(var k in arr[0]){
			cols.push(k);
		}
		if (add_col)
			rows.push( cols.join("\t") );

		for(var i = 0; i < arr.length; i++){
			var row = [];
			for(var j=0; j < cols.length; j++) 
				row.push( arr[i][ cols[j] ] );
			rows.push( row.join("\t") );
		}
		return rows.join("\n");
	}

	this.to_html_tbl = function(arr, add_col){
		var tmp_arr=[];
		if ( arr.hasOwnProperty("length") == false ){ //it's an object of objects, convert to arr
			for(var k in arr) tmp_arr.push( arr[k] );
			arr = [].concat( tmp_arr );
		}
		var rows =[];
		var cols = [];

		if (!arr.length) return [];

		for(var k in arr[0]) cols.push(k);
		if (add_col)
			rows.push("<tr><th>"+cols.join("</th><th>")+"</th></tr>");

		for(var i = 0; i < arr.length; i++){
			var row = [];
			for(var j=0; j < cols.length; j++) 
				row.push( arr[i][ cols[j] ] );
			
			rows.push("<tr><td>"+row.join("</td><td>")+"</td></tr>");
		}
		return "<table class='tbl2'>"+rows.join("\n")+"</table>";
	}




	/*
		return items in a not in b
	*/
	this.array_diff = function(a,b){
		var list = [];
		for(var i = 0; i < a.length; i++){
			if ( b.indexOf( a[i] ) == -1 ) list.push(a[i]);
		}
		return list;
	}


	this.q_fns = function(fns,cb){
		if ( fns.length == 0 ) return cb();

		var fn = fns.pop();
		return fn( function(){ self.q_fns(fns,  cb); });
	}
	this.exec_mongo = function(path_to_script,db,cb){
		switch(db){
			case "tmNotes":
				var par1 = "mongo:27017/tmNotes";
				break;
			case "sop_tracking":
				var par1 = "mongo:27017/sop_tracking";
				break;
			default:
				break;
		}
		var p = spawn('/usr/local/bin/mongo', [par1, "--quiet", path_to_script ]);
		var out = "";
		p.stdout.on("data", function(data){ out+=data;	});
		p.stderr.on("data", function(data){ out+=data;	});
		p.on("exit",function(code){
			try {
				if (!out) var pkg={status:false, code:code};
				else var pkg= JSON.parse(out);
			}
			catch(err){
				return cb({err:err,out:out,code:code},false);
			}
			return cb(null,pkg);
		});
	}


	this.convert_args_to_params = function( args, sql_field_name ){
		var pkg = { ins:[], wilds:[], regex:[]  };
		for( var i = 0;  i <  args.length ; i ++ ){ 

			var param = args[i];
			if ( param.search(/\*/g) !== -1 ){

				pkg.wilds.push( "'" + param.replace(/\*/g,"%") + "'" );

				if ( param.substr(0,1) !== "*" )
					param = "^" + param;
				else
					param = param.substr(1);

				if ( param.substr(-1,1) !== "*" )
					param += "$";
				else
					param = param.substr(0,param.length-1);

				pkg.regex.push( param.replace(/\*/g,".*") );
			}
			else
				pkg.ins.push(param);
		}


		var crit = {};
		var sql = [];
		if ( pkg.ins.length  && pkg.regex.length ){
			var rg = new RegExp( pkg.regex.join("|") ,"g");
			crit["$or"] = [ 
				{ _id: {$in: pkg.ins} },
				{ $regex: rg }
			];
		}
		else if ( pkg.ins.length ){
			crit["_id"] = {$in: pkg.ins };
		}
		else if ( pkg.regex.length ){
			var rg = new RegExp( pkg.regex.join("|") ,"g");
			crit["_id"] = { $regex: rg };
		}

		if ( sql_field_name ){
			if ( pkg.ins.length  && pkg.wilds.length ){
				sql.push(" and ( " );
				sql.push(" " + sql_field_name + " in ('"+pkg.ins.join("','")+"') " );
				sql.push(" or " + sql_field_name + " like " + pkg.wilds.join(" or " + sql_field_name + " like " ) );
				sql.push(" ) ");
			}
			else if ( pkg.ins.length ){
				sql.push(" and " + sql_field_name + " in ('"+pkg.ins.join("','")+"') " );
			}
			else if ( pkg.wilds.length ){
				sql.push(" and ( " );
				sql.push(" " + sql_field_name + " like " + pkg.wilds.join(" or " + sql_field_name + " like " ));
				sql.push(" ) ");
			}


			sql = sql.join("");
		}

		pkg.crit = crit;
		pkg.sql = sql;
		return pkg;
	}


	/*
		all files in a directory
		strip out bad chars	
	*/
	this.normalize_files = function( in_path, cb){
		if ( in_path.substr(-1) !== "/" ) in_path+="/";
		fs.readdir( in_path ,  function(err, fns){
			if ( err ) {
				l(err);
				process.exit();
				return cb(err);
			}

			var fromtos = [];
			for(var i = 0; i  < fns.length; i++){
				var nm = fns[i];
			
				var from = in_path + nm.replace(/ /g,"\ ");
				var to = in_path + nm.replace(/[ @]/g,"_");
				l(from,to);

				if ( from !== to ) {
					fromtos.push( [ from, to ] );
				}
			}
			return self.move_one_file( fromtos, cb );	
		});
	}

	/*
		all files in a directory
		@ implies remote
	*/
	this.move_files = function( from_path, to_path , cb){
		if ( from_path.substr(-1) !== "/" ) from_path+="/";
		if ( to_path.substr(-1) !== "/" ) to_path+="/";

		fs.readdir( from_path ,  function(err, fns){
			if ( err ) {
				l(err);
				process.exit();
				return cb(err);
			}

			var fromtos = [];
			for(var i = 0; i  < fns.length; i++){
				var nm = fns[i];
				var from = from_path+nm;
				var to = to_path+nm;
				fromtos.push( [ from, to ] );
			}
			return self.move_one_file( fromtos, cb );	
		});
	}

	this.move_one_file = function(from_tos , cb){
		if ( from_tos.length == 0 ) return cb(null);

		var from_to = from_tos.pop();	
		var from = from_to[0];
		var to = from_to[1];

		var psTxt="";
		l("moving: " , from , to );
		var ps_p = spawn('mv', [from, to ]);
		ps_p.stderr.on("data",function(data){ 
			psTxt+=data.toString(); 
			l(psTxt);
		});
		ps_p.on("exit",function(code){
			if (code!=0){
				//error with sql
				l('mv errror');
				l(psTxt);
				process.exit();
			}
			return self.move_one_file( from_tos, cb);
		});
	}
	


	/*
		universal mover
	*/
	this.move_all_files_in_path = function(from,to, cb){





		self.run_bash(self.config.listing_bash_file,function(err,psTxt){
			l(err);
			if ( !psTxt ) return self.tmu.proc_done();

			var out_lines = psTxt.split(/\n/g);
			var to_remove = [];
			for(var i=0; i < out_lines.length; i++){
				var parts = out_lines[i].split(/\//g);
				var base_name = parts[ parts.length -1 ];
				if ( base_name ){
					l("got " + base_name);
					if ( base_name ){
						base_name = base_name.replace(/ /g, "\\ ");
						l(base_name);
						to_remove.push( "ssh "+ self.config.source_access +" 'rm " + self.config.source_inbox + "/" + base_name + "'");
					}
				}
			}
			if ( to_remove.length == 0 ) return self.tmu.proc_done();

			var contents = to_remove.join("\n");
			var bash_clear_file = "/tmp/"+(new Date()).getTime().toString() + "_clear_"+self.config.group_nm+".bash";
			l(bash_clear_file);
			self.tmu.write( bash_clear_file , contents, function(err){
				fs.chmod(bash_clear_file,'755', function(err){
					l("executing bash: " + bash_clear_file);
					self.tmu.run_bash(bash_clear_file ,function(err,psTxt){
						l("end of bash to remove");
					});
				});
			});

			//normalize the newly arrived local files
			return self.tmu.normalize_files( self.config.inbound_path, self.tmu.proc_done);
		});
	}


	this.move = function(from, to, cb){
	
		var from_pkg = self._parse_smart_path( from ); 
		var to_pkg = self._parse_smart_path( to ); 
		
		/*
			possibilities
						from:	
						local	ssh
			to:			_ _ _ _ _ _ _ _ _ _ _
			local	|	  x	
			ssh		|	  	



		*/
				
		

		l("\nAttempting scp " + from + " to " + to + "\n");
		var ps_p = spawn(
			'/usr/bin/scp', 
			['-i', pem, from, to ]
		);
		var psTxt="", psErr="";
		ps_p.stdout.on("data",function(data){ psTxt+=data.toString(); });
		ps_p.stderr.on("data",function(data){ psErr+=data.toString(); });
		ps_p.on("exit",function(){
			if ( psErr ) return cb(psErr);
			cb(null,psTxt,"completed scp " + from + " to " + to);
		});


	}








	/*
		local fn:  "/path/to/local/file"
		remote fn: "/path/on/remote/server/to/local/filename@hostname"
	*/
	this.write = function( fn, data, cb){
		var parts = fn.split(/@/);
		if ( parts.length > 1 ){
			var path = parts[0];
			var host = parts[1];
			var remote_fn = "root@" + host + ':"' + path + '"';
			var local_tmp_fn = "/tmp/"  + (new Date()).getTime().toString() + "_" + path.split("/").pop();

			fs.writeFile(local_tmp_fn , data, function(err){
				if ( err ) 
					return cb(err);

				var psTxt="";
				var ps_p = spawn('scp', [local_tmp_fn, remote_fn]);
				ps_p.stdout.on("data",function(data){ 
					psTxt+=data.toString(); 
				});

				ps_p.stderr.on("data",function(data){ 
					psTxt+=data.toString(); 
				});
				ps_p.on("exit",function(code){
					if (code!=0){
						//error with sql
						l('scp errror');
						l(psTxt);
						return cb(psTxt);
					}
					return cb(null);
				});
			});


		}
		else {
			return fs.writeFile(fn, data, function(err, result){
				return cb(err, result);
			});
		}

	}

	this.run_bash = function(bashfile,cb){
		var psTxt="";
		var ps_p = spawn(bashfile,[]); 
		ps_p.stdout.on("data",function(data){ 
			psTxt+=data.toString(); 
		});

		ps_p.stderr.on("data",function(data){ 
			psTxt+=data.toString(); 
		});
		ps_p.on("exit",function(code){
			if (code!=0){
				//error with sql
				l('scp errror');
				l(psTxt);
				return cb(psTxt);
			}
			return cb(null, psTxt);
		});
	}



	/*
		local fn:  "/path/to/local/file"
		remote fn: "/path/on/remote/server/to/local/filename@hostname"
	*/
	this.read = function( fn, cb){
		var parts = fn.split(/@/);
		if ( parts.length > 1 ){
			var path = parts[0];
			var host = parts[1];
			var from = "root@" + host + ':"' + path + '"';
			var local_fn = "/tmp/"  + (new Date()).getTime().toString() + "_" + path.split("/").pop();
			var psTxt="";
			var ps_p = spawn('scp', [from, local_fn] );
			ps_p.stdout.on("data",function(data){ 
				psTxt+=data.toString(); 
			});

			ps_p.stderr.on("data",function(data){ 
				psTxt+=data.toString(); 
			});
			ps_p.on("exit",function(code){
				if (code!=0){
					//error with sql
					l('scp errror');
					l(psTxt);
					return cb(psTxt);
				}

				return fs.readFile(local_fn, function(err, data){
					return cb(err, data);
				});
			});
		}
		else {
				
			return fs.readFile(fn, function(err, data){
				return cb(err, data);
			});
		}
	}


	/*
		local fn:  "/path/to/local/file"
		remote fn: "/path/to/remote/file@hostname"
	*/
	this.copy = function( from, to, cb){
		var from_parts = from.split(/@/);
		var to_parts = to.split(/@/);
		
		if ( from_parts.length == 0 || to_parts.length == 0 ){
			var ps_p = spawn('cp', [from, to] );
			ps_p.on("exit",function(code){
				if (code!=0){
					//error with sql
					return cb({msg:"Failed local to local copy",from:from, to:to});
				}
				return cb(err, data);
			});
				
		}

		if ( to_parts.length > 1 ){
			var path = to_parts[0];
			var host = to_parts[1];
			var to = "root@" + host + ':"' + path + '"';
		}

		if ( from_parts.length > 1 ){
			var path = from_parts[0];
			var host = from_parts[1];
			var from = "root@" + host + ':"' + path + '"';
		}

		//l("scp",from, to);

		var ps_p = spawn('scp', ["-r", from, to] );
		var psTxt = "";
		ps_p.stdout.on("data",function(data){ 
			psTxt+=data.toString(); 
		});
		ps_p.stderr.on("data",function(data){ 
			psTxt+=data.toString(); 
		});
		ps_p.on("exit",function(code){
			if (code!=0){
				//error with sql
				l("copy failed " + from + " " + to , code, psTxt);
				return cb({msg:"Failed scp",from:from,to:to});
			}
			l("copy completed " + from + " " + to);
			return cb(null, psTxt);
		});
	}



	//graceful termination
	this.end = function(a){ 
		if (a){
			l("Exiting no errors",a); 
			process.exit(); 
		}
		process.exit(); 
	}

	this.q_procs = function(arr,delay){
    if ( delay===undefined) self.q_proc_delay = 1;
		if ( arr.length == 0 ) 
			self.crash({ msg:"Bad proc list" , no_warn: true}); 


		if ( self.data.proc_list){
			self.crash({ msg:"proc list already exists", no_warn:true }); 
		}

		self.data.proc_list = arr;
		//l("q'd process list:");
		/*
		for(var i = 0; i < self.data.proc_list.length; i++)
			l(typeof self.data.proc_list[i]);
		*/
		self.proc_done();
	};

	this.proc_done = function(err,pkg){
		if ( self.data.proc_list == undefined ) {
			return process.exit();
		}

		if ( self.data.proc_list.length == 0 ) {
			//l("Last proc completed " + (new Date()).toString());
			delete self.data.proc_list;
			if ( "function" === typeof self.after_last_proc )
				return self.after_last_proc();
			//return process.exit();
		}
		else{
			//l(self.data.proc_list.length + " left in procs list");	
			setTimeout(function(){
				return self.data.proc_list.splice(0,1)[0](err,pkg);
			},self.q_proc_delay);
		}
	}


	this.notify = function(pkg,cb){

		/*
		var ff = {
			from: from,
			to: to,
			subject: subject,
			text: text
		};
		*/
		var m = mailgun.messages();
		m.send(pkg,function(err,body){
			if ( err ) l(err,body);
			return cb(err,body);
		});
	}

	this.crash = function(pkg){
		pkg.level="WARN:";

		if ( pkg.no_warn === true ) {
			l(pkg);
			return process.exit();
		}

		l(pkg);
		return self.notify(pkg, process.exit);
	}
	this.die = function(pkg){
		l(pkg);
		process.exit();
	}

	this.in_array = function(needle, haystack){
		if ( haystack.indexOf(needle) == -1 ) return false;
		return true;
	}

	this.pull_ids = function(arr_of_obs, id_col_names){
		var ids=[];
		arr_of_obs.forEach(function(a){
			ids.push( a[id_col_names] );
		});
		return ids;
	}

	
	/*
		assumes no repeat of id_col_names
		id_col_names -> array of field names that must exist in each object in arr_of_obs
		child_arrs -> array of names of new fields to attach to each object in arr of obs, set to an empty array
	*/
	this.pull_ids_build_index = function(arr_of_obs, id_col_names, cross_ref, child_arrs ){
		var brick = { ids: { }, maps: { }, bad_ids: { }, cross_ref:{} };
		for(var i = 0; i < id_col_names.length; i++){
			brick.ids[ id_col_names[i] ] = [];
			brick.bad_ids[ id_col_names[i] ] = [];
			brick.maps[ id_col_names[i] ] = { };
		}

		arr_of_obs.forEach(function(a, idx){
			for(var i = 0; i < id_col_names.length; i++){
				if ( !a[id_col_names[i]] ){
					l("\n\n BAD ID VALUE: ", a);
					brick.bad_ids[ id_col_names[i] ].push( a );
				}
				else{
					brick.ids[ id_col_names[i] ].push( a[id_col_names[i]] );
					brick.maps[ id_col_names[i] ][  a[id_col_names[i]].toString() ] = idx;
				}
			}

			if ( cross_ref ){
				for(var k in cross_ref)
					if ( a[ k ] ){
						if ( undefined == brick.cross_ref[ a[k].toString() ] )
							brick.cross_ref[ a[k].toString() ] = [];
						brick.cross_ref[ a[k].toString() ].push( a[ cross_ref[k] ] );
					}
			}


			for(var k in child_arrs){
				if ( typeof child_arrs[k] == "object" )
					arr_of_obs[idx][ k ] = new Array();
				else
					arr_of_obs[idx][ k ] = child_arrs[k];
			}
		});
		brick.source= arr_of_obs;
		return brick;
	}

	return this;
}


