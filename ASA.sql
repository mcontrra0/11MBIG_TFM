WITH p1 AS (
    SELECT 
        ds_id                   id,
        ds_id                   patient_1_id,
        gender                  patient_1_gender,
        age                     patient_1_age,
        type                    patient_1_type,
        blood_glucose           patient_1_blood_glucose,
        heart_rate              patient_1_heart_rate,
        EventProcessedUtcTime   patient_1_time,
        UDF.getTime('')         patient_1_formattedTime
    FROM
        [event-hub-diabetes-monitoring]
    WHERE
        patient = 1
), p2 AS (
    SELECT 
        ds_id                   patient_2_id,
        gender                  patient_2_gender,
        age                     patient_2_age,
        type                    patient_2_type,
        blood_glucose           patient_2_blood_glucose,
        heart_rate              patient_2_heart_rate,
        EventProcessedUtcTime   patient_2_time,
        UDF.getTime('')         patient_2_formattedTime
    FROM
        [event-hub-diabetes-monitoring]
    WHERE
        patient = 2
), p3 AS (
    SELECT 
        ds_id                   patient_3_id,
        gender                  patient_3_gender,
        age                     patient_3_age,
        type                    patient_3_type,
        blood_glucose           patient_3_blood_glucose,
        heart_rate              patient_3_heart_rate,
        EventProcessedUtcTime   patient_3_time,
        UDF.getTime('')         patient_3_formattedTime
    FROM
        [event-hub-diabetes-monitoring]
    WHERE
        patient = 3
), p4 AS (
    SELECT 
        ds_id                   patient_4_id,
        gender                  patient_4_gender,
        age                     patient_4_age,
        type                    patient_4_type,
        blood_glucose           patient_4_blood_glucose,
        heart_rate              patient_4_heart_rate,
        EventProcessedUtcTime   patient_4_time,
        UDF.getTime('')         patient_4_formattedTime
    FROM
        [event-hub-diabetes-monitoring]
    WHERE
        patient = 4
)

SELECT 
        ds_id id,
        patient,
        gender,
        age,
        type,
        blood_glucose,
        heart_rate,
        EventProcessedUtcTime,
        UDF.getTime(''),
        50 as blood_glucose_min,
        200 as blood_glucose_max,
        70 as blood_glucose_goal,
        50 as heart_rate_min,
        130 as heart_rate_max,
        70 as heart_rate_goal
INTO    [cosmosdb-diabetes-monitoring]
FROM    [event-hub-diabetes-monitoring] 


SELECT 
        p1.id,
        p1.patient_1_blood_glucose,
        p1.patient_1_time,
        p2.patient_2_blood_glucose,
        p2.patient_2_time,
        p3.patient_3_blood_glucose,
        p3.patient_3_time,
        p4.patient_4_blood_glucose,
        p4.patient_4_time,
        50 as blood_glucose_min,
        200 as blood_glucose_max,
        70 as blood_glucose_goal,
        50 as heart_rate_min,
        130 as heart_rate_max,
        70 as heart_rate_goal
INTO    [powerbi-diabetes-monitoring]
FROM    p1 
JOIN    p2 ON p1.patient_1_id = p2.patient_2_id AND DateDiff(second,p1,p2) between 0 and 2
JOIN    p3 ON p2.patient_2_id = p3.patient_3_id AND DateDiff(second,p3,p2) between 0 and 2
JOIN    p4 ON p4.patient_4_id = p3.patient_3_id AND DateDiff(second,p3,p4) between 0 and 2