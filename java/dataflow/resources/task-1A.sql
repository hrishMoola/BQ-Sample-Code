# task 1A
CREATE TEMP FUNCTION parseRequest(request STRING)
RETURNS STRUCT<uuid STRING, os STRING, event_id STRING, amount FLOAT64, store STRING, bundle STRING>
LANGUAGE js AS """
   var queryParams =request.split("?");
  var queries = queryParams[1].split("&");
  let paramsMap = new Map();
  for(i = 0 ; i < queries.length; i ++){
    paramsMap.set(queries[i].split("=")[0], queries[i].split("=")[1]);
  }
  if(paramsMap.has("gps_adid")){
    this.uuid = paramsMap.get("gps_adid");
    this.os = "android";
  }else if(paramsMap.has("ios_idfa")){
    this.os = "ios";
    this.uuid = paramsMap.get("ios_idfa");
  }
  this.amount = paramsMap.get("amount");
  this.event_id = paramsMap.get("event_id");
  this.store = paramsMap.get("store");
  this.bundle = paramsMap.get("bundle");
  return this;

""";

with httpResults as
(SELECT httpRequest.*, timestamp FROM `cs-686-lab06.loadBalancerLogs.loadBalancerLogs_small`),

 parsedTable as (select parseRequest(requestUrl) as request, timestamp as time from httpResults)

Select * from (
select bundle, ANY_VALUE(purchases) as total_amount, count(distinct(id)) as distinct_ids from (
SELECT request.uuid as id, request.os, request.event_id, SUM(request.amount) OVER(partition by request.bundle) as purchases
, request.store, request.bundle as bundle FROM parsedTable )
group by 1
order by distinct_ids desc
)
where distinct_ids >=10