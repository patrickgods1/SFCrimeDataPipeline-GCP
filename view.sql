CREATE MATERIALIZED VIEW `sf-crime-data-pipeline.sf_crime_data_dataset.Dashboard_View` AS (
SELECT fact_crime.`Incident_Description` AS IncidentDescription,
        fact_crime.Latitude,
        fact_crime.Longitude,
        fact_crime.`Incident_Date` AS IncidentFullDate,
        dim_date.year AS IncidentYear,
        dim_date.month_name AS IncidentMonth,
        dim_date.day_name AS IncidentDayOfWeek,
        dim_time.hour24 AS IncidentHour,
        dim_date.holiday_name AS IncidentHolidayName,
        dim_date.is_weekday AS IncidentisWeekend,
        fact_crime.`Incident_Time` AS IncidentFullTime12,
        dim_time.timeOfDay AS IncidentTimeOfDay,
        fact_crime.Intersection,
        fact_crime.`Police_District` AS PoliceDistrict,
        fact_crime.`Analysis_Neighborhood` AS AnalysisNeighborhood,
        fact_crime.`Incident_Category` AS IncidentCategory,
        fact_crime.`Incident_Subcategory` AS IncidentSubcategory,
        fact_crime.`Report_Type_Description` AS ReportType
FROM `sf-crime-data-pipeline.sf_crime_data_dataset.SFCrimeData2018toPresent` fact_crime
JOIN `sf-crime-data-pipeline.sf_crime_data_dataset.dim_date` dim_date
    ON fact_crime.`Incident_Date` = dim_date.date
JOIN `sf-crime-data-pipeline.sf_crime_data_dataset.dim_time` dim_time
    ON fact_crime.`Incident_Time` = dim_time.fullTime24
);