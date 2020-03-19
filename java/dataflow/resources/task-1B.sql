# task 1B
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
CREATE TEMP FUNCTION
  urldecode(url string) AS ((
    SELECT
      SAFE_CONVERT_BYTES_TO_STRING( ARRAY_TO_STRING(ARRAY_AGG(
          IF
            (STARTS_WITH(y, '%'),
              FROM_HEX(SUBSTR(y, 2)),
              CAST(y AS BYTES))
          ORDER BY
            i ), b''))
    FROM
      UNNEST(REGEXP_EXTRACT_ALL(url, r"%[0-9a-fA-F]{2}|[^%]+")) AS y
    WITH
    OFFSET
      AS i ));
with httpResults as
(SELECT httpRequest.*, timestamp FROM `cs-686-lab06.loadBalancerLogs.loadBalancerLogs_medium`),

 parsedTable as (select parseRequest(urldecode(requestUrl)) as request, timestamp as time from httpResults)

select * from (
Select bundle, (ANY_VALUE(total_amount) / ANY_VALUE(distinct_ids)) as average from (
select count(distinct(id)) as distinct_ids, ANY_VALUE(purchases) as total_amount, bundle from (
SELECT request.uuid as id, request.os, request.event_id, SUM(request.amount) OVER(partition by request.bundle) as purchases
, request.store, request.bundle as bundle FROM parsedTable )
group by 3
order by distinct_ids desc
)
group by 1
)
where average >=10000