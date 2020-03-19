# task 3A
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

CREATE TEMP FUNCTION checkConsec(events ARRAY<int64>, days int64)
RETURNS bool
LANGUAGE js AS """
  let mySet = new Set();
  for(i = 0 ; i < events.length ; i ++){
    mySet.add(events[i]);
  }
  let sortedArray = Array.from(mySet).sort();
  var consecCounter = 1;
  var maxConsec = 1;
  if(sortedArray.length >= days){
    for(i = 0 ; i < sortedArray.length - 1 ; i ++){
      if(sortedArray[i+1] - sortedArray[i] == 1){
        consecCounter++;
        maxConsec = Math.max(maxConsec, consecCounter);
      }
    }
    return maxConsec >= days;
  }
  return false;
""";
with httpResults as
(SELECT httpRequest.*, timestamp FROM `cs-686-lab06.loadBalancerLogs.loadBalancerLogs_medium`),

 parsedTable as (select parseRequest(requestUrl) as request, timestamp as time from httpResults),

 possibleData as (
select * from(
select os, id, time, total_counts, SUM(total_amount) as totalAmount from (
SELECT request.os as os, time, request.uuid as id, SUM(request.amount) OVER(partition by request.bundle, request.uuid, time order by request.uuid) as total_amount, COUNT(request.uuid) OVER(partition by request.bundle,  request.uuid) as total_counts ,request.bundle as bundle
-- FROM `cs-686-lab06.loadBalancerLogs.loadBalancerLogs_parsed_medium`
FROM parsedTable
where request.amount > 0.0 AND request.bundle = 'id486686'
) group by 1, 2, 3, 4)
-- order by 1 desc)
where total_counts >= 3
order by id
)
select os, id  from (
select id, os, ARRAY_CONCAT_AGG(groupedData.day), CheckConsec(ARRAY_CONCAT_AGG(groupedData.day), 3) as whaa from (
select data.id as id, data.os as os,
struct(array_agg(data.totalAmount) as totalAmount, array_agg(EXTRACT (day from data.time)) as day ) as groupedData
from (select  STRUCT(id, os, total_counts, totalAmount, time) as data from possibleData)
group by 1,2
)
group by 1,2)
where whaa = true;