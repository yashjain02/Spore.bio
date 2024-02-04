

CREATE SCHEMA IF NOT EXISTS spore;

CREATE TABLE IF NOT EXISTS spore.membrane_dimension (
    membrane_name TEXT PRIMARY KEY, 
    barcode TEXT, 
    row_num INT,
    biosample_position DECIMAL,
    matrix_tested_inprogress VARCHAR, 
    experiment_name TEXT, 
    usable_for_ml BOOLEAN,
    exclusion_reason VARCHAR
);

CREATE TABLE IF NOT EXISTS spore.images_dimension (
    image_name TEXT PRIMARY KEY,
    barcode TEXT,
    matrix_tested_inprogress VARCHAR, experiment_name TEXT, usable_for_ml BOOLEAN,
    exclusion_reason VARCHAR
);


CREATE TABLE IF NOT EXISTS spore.camera_dimension(optical_setup TEXT PRIMARY KEY, lens_diameter DECIMAL, objective VARCHAR, camera VARCHAR );

CREATE TABLE IF NOT EXISTS spore.date_dimension(filtration_date DATE PRIMARY KEY, Date_day int, date_month int, date_year int);
CREATE TABLE IF NOT EXISTS spore.membrane_image_camera (
    membrane VARCHAR, FOREIGN KEY (membrane) REFERENCES spore.membrane_dimension(membrane_name),
    image_name VARCHAR, FOREIGN KEY (image_name) REFERENCES spore.images_dimension(image_name),
    optical_setup VARCHAR ,FOREIGN KEY (optical_setup) REFERENCES spore.camera_dimension(optical_setup),
    nomenclature_format DECIMAL, number_of_bacteria_pixels BIGINT,
    acquisitions_realized INT, number_of_acquisitions INT, filtration_date DATE,
    FOREIGN KEY (filtration_date) REFERENCES spore.date_dimension(filtration_date),
    matrix_dilution VARCHAR, types_of_microorganisms VARCHAR, ecoli_percentage DECIMAL(5,2),
    pseudomonas_percentage DECIMAL(5,2), pretreatment_operator VARCHAR, total_number_of_bacteria_measured_in_lab DECIMAL
);