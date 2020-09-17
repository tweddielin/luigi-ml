-- Get the average precision for the K-fold
SELECT e.avg, m.model_uuid, m.class_path 
FROM (
    SELECT avg(vaavg, max(model_uuid) as model_uuid 
    From results.evaluation where metric='precision@' and parameter='all' group by model_group_id
    ) as e 
LEFT JOIN results.models m 
ON m.model_uuid = e.model_uuid 
ORDER BY avg DESC 
LIMIT 10;
