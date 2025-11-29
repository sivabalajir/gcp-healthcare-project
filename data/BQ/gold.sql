-- 1. Total Charge Amount per provider by department
CREATE TABLE IF NOT EXISTS `long-nation-476207-n7.gold_dataset.provider_charge_summary` (
    Provider_Name STRING,
    Dept_Name STRING,
    Amount FLOAT64
);

-- truncate table
TRUNCATE TABLE `long-nation-476207-n7.gold_dataset.provider_charge_summary`;

-- insert data
INSERT INTO `long-nation-476207-n7.gold_dataset.provider_charge_summary`
SELECT 
    CONCAT(p.firstname, ' ', p.LastName) AS Provider_Name,
    d.Name AS Dept_Name,
    SUM(t.Amount) AS Amount
FROM `long-nation-476207-n7.silver_dataset.transactions` t
LEFT JOIN `long-nation-476207-n7.silver_dataset.providers` p 
    ON SPLIT(p.ProviderID, "-")[SAFE_OFFSET(1)] = t.ProviderID
LEFT JOIN `long-nation-476207-n7.silver_dataset.departments` d 
    ON SPLIT(d.Dept_Id, "-")[SAFE_OFFSET(0)] = p.DeptID
WHERE t.is_quarantined = FALSE 
  AND d.Name IS NOT NULL
GROUP BY Provider_Name, Dept_Name;
--------------------------------------------------------------------------------------------------


-- 2. Patient History (Gold) : This table provides a complete history of a patient’s visits, diagnoses, and financial interactions.

-- CREATE TABLE
CREATE TABLE IF NOT EXISTS `long-nation-476207-n7.gold_dataset.patient_history` (
    Patient_Key STRING,
    FirstName STRING,
    LastName STRING,
    Gender STRING,
    DOB INT64,
    Address STRING,
    EncounterDate INT64,
    EncounterType STRING,
    Transaction_Key STRING,
    VisitDate INT64,
    ServiceDate INT64,
    BilledAmount FLOAT64,
    PaidAmount FLOAT64,
    ClaimStatus STRING,
    ClaimAmount STRING,
    ClaimPaidAmount STRING,
    PayorType STRING
);

-- TRUNCATE TABLE
TRUNCATE TABLE `long-nation-476207-n7.gold_dataset.patient_history`;

-- INSERT DATA
INSERT INTO `long-nation-476207-n7.gold_dataset.patient_history`
SELECT 
    p.Patient_Key,
    p.FirstName,
    p.LastName,
    p.Gender,
    p.DOB,
    p.Address,
    e.EncounterDate,
    e.EncounterType,
    t.Transaction_Key,
    t.VisitDate,
    t.ServiceDate,
    t.Amount AS BilledAmount,
    t.PaidAmount,
    c.ClaimStatus,
    c.ClaimAmount,
    c.PaidAmount AS ClaimPaidAmount,
    c.PayorType
FROM `long-nation-476207-n7.silver_dataset.patients` p
LEFT JOIN `long-nation-476207-n7.silver_dataset.encounters` e 
    ON SPLIT(p.Patient_Key, '-')[OFFSET(0)] || '-' || SPLIT(p.Patient_Key, '-')[OFFSET(1)] = e.PatientID
LEFT JOIN `long-nation-476207-n7.silver_dataset.transactions` t 
    ON SPLIT(p.Patient_Key, '-')[OFFSET(0)] || '-' || SPLIT(p.Patient_Key, '-')[OFFSET(1)] = t.PatientID
LEFT JOIN `long-nation-476207-n7.silver_dataset.claims` c 
    ON t.SRC_TransactionID = c.TransactionID
WHERE p.is_current = TRUE;
--------------------------------------------------------------------------------------------------
