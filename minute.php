<?php


function readView($params) {
    $result = array();
    $offset=0;
    $limit=100000;
    $mres = [];
    do {
        $url = 'http://readprices:readprices@10.0.10.3:5984/cryptowatch/_design/queries/_view/prices?update=lazy&skip='.$offset.'&limit='.$limit.'&'.$params;
        $res = file_get_contents($url,false);
        $mres = json_decode($res)->rows;
	foreach ($mres as $row) {
	    $result[] = "$row->id:$row->value";
	}
	$offset+=$limit;
    } while (count($mres) ==  $limit);
    return $result;
}
function readViewReduce($params) {
    $result = array();
    $offset=0;
    $limit=100000;
    $mres = [];
    do {
        $url = 'http://readprices:readprices@10.0.10.3:5984/cryptowatch/_design/queries/_view/prices?update=lazy&skip='.$offset.'&limit='.$limit.'&'.$params;
        $res = file_get_contents($url,false);
        $mres = json_decode($res)->rows;
	foreach ($mres as $row) {
	    $result[] = "$row->key"."~"."$row->value";
	}
	$offset+=$limit;
    } while (count($mres) ==  $limit);
    return $result;
}

function generateRandomString($length = 10) {
    return substr(str_shuffle(str_repeat($x='0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', ceil($length/strlen($x)) )),1,$length);
}

function base64url_encode($data) {
  return rtrim(strtr(base64_encode($data), '+/', '-_'), '=');
}

function genSignature($str) {
    return base64url_encode(hash_hmac('md5', $str, 'p2ipqowqpowekiepwodkwq', TRUE));
}	
/*

$token=@$_GET["token"];
if (!isset($token)) {
	header("HTTP/1.0 401 Authorization required");	
	header('Content-Type: application/json');
	
	$token_str = generateRandomString(20);
	$token_sign = genSignature($token_str);
	
	$out = array("token"=>$token_str.$token_sign,
		     "desc"=>"If you aggree with these rules, append &token=xxx to the request, where xxx is value of the field 'token'",
		     "licence"=>"To use this data source, you need to obey following rules:\n\n".
		     		"* Data on this site are provided 'as is' without warranty for the correctness and accuracy. The service provider is not responsible for errors in the data and is not responsible for any damages that these errors have caused or will cause in the future.\n".
		     		"* It is allowed to use the data for any purpose\n".
		     		"* It is forbidden to overload the server with too frequent requests sent with an interval of less than 1 request in 10 seconds. It is forbidden to send requests for one pair (pooling) in an interval shorter than 1 request per minute. It is forbidden to download data that is not used for further processing.\n"
		     );
		     
	echo json_encode($out);	
		

} else*/ {
	/*$token_str = substr($token,0,20);
	$token_sign = genSignature($token_str);
	if ($token_str.$token_sign != $token) {
		header("HTTP/1.0 403 Authorization required");	
		header('Content-Type: application/json');
		
		echo json_encode(array("error"=>"invalid token"));
	} else {
*/
	$asset=@$_GET["asset"];
	$currency=@$_GET["currency"];
	$start_time=@$_GET["from"];
	$end_time=@$_GET["to"];

	header('Content-Type: application/json');



	if (!isset($asset) && !isset($currency)) {

	    $res = readViewReduce("group=true");
	    $sep = "{";
	    $initsep= $sep;
		foreach($res as $v) {
		    list($key,$value) = explode("~",$v);
		    echo $sep,'"',$key,'":',$value;
		$sep = ",\n";
		$initsep="";
	    }
	    echo $initsep,'}';



	} else {



		if (!isset($start_time)) {
		    $start_time="0";
		} else {
		    $start_time=substr($start_time,0,9);
		}

		if (!isset($end_time)) {
		    $end_time="999999999";
		} else {
		    $end_time=substr($end_time,0,9);
		}



		$assets = readView('reduce=false&start_key="'.urlencode($asset).'"&end_key="'.urlencode($asset).'"&startkey_docid='.$start_time.'&endkey_docid='.$end_time);
		$currencies = readView('reduce=false&start_key="'.urlencode($currency).'"&end_key="'.urlencode($currency).'"&startkey_docid='.$start_time.'&endkey_docid='.$end_time);

		$asz = count($assets);
		$csz = count($currencies);

		$aidx = 0;
		$cidx = 0;
		$sep = "[";
		$initsep= $sep;
		while ($aidx< $asz && $cidx < $csz) {
		    list($aid,$astr) = explode(":",$assets[$aidx]);
		    list($cid,$cstr) = explode(":",$currencies[$cidx]);
		    if ($aid < $cid) $aidx++;
		    else if ($aid > $cid) $cidx++;
		    else {
			$a = floatval($astr);
			$c = floatval($cstr);
			$r = $a/$c;
			echo $sep,"[",$aid,"0,",$r,"]";
			$sep=",\n";
			$aidx++;
			$cidx++;
			$initsep="";
		    }
		}
		echo $initsep,"]";

		}
//	}
}
?>
