

CREATE SCHEMA IF NOT EXISTS spore;

CREATE TABLE IF NOT EXISTS spore.membrane_table (
    membrane_name TEXT PRIMARY KEY, 
    barcode TEXT, 
    row_num INT,
    nomenclature_format DECIMAL, 
    biosample_position DECIMAL,
    matrix_tested_inprogress VARCHAR, 
    experiment_name TEXT, 
    usable_for_ml BOOLEAN,
    exclusion_reason VARCHAR, 
    total_number_of_bacteria_measured_in_lab DECIMAL,
    acquisitions_realized INT, 
    number_of_acquisitions INT, filtration_date DATE,
    matrix_dilution VARCHAR, types_of_microorganisms VARCHAR, ecoli_percentage DECIMAL,
    pseudomonas_percentage DECIMAL, pretreatment_operator VARCHAR
);

CREATE TABLE IF NOT EXISTS spore.images_table (
    image_name TEXT PRIMARY KEY,
    barcode TEXT,
    membrane TEXT,
    FOREIGN KEY (membrane) REFERENCES spore.membrane_table(membrane_name),
    nomenclature_format DECIMAL,
    matrix_tested_inprogress VARCHAR, experiment_name TEXT, usable_for_ml BOOLEAN,
    exclusion_reason VARCHAR,   total_number_of_bacteria_measured_in_lab DECIMAL,
    number_of_bacteria_pixels BIGINT,
    acquisitions_realized INT, number_of_acquisitions INT, filtration_date DATE,
    matrix_dilution VARCHAR, types_of_microorganisms VARCHAR, ecoli_percentage DECIMAL,
    pseudomonas_percentage DECIMAL, pretreatment_operator VARCHAR,optical_setup VARCHAR,
    lens_diameter DECIMAL, objective VARCHAR, camera VARCHAR
);
