CREATE TABLE track_employees(
    track_employee_id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
    employee_id INT(11) UNSIGNED NOT NULL,
    hire_date DATE NOT NULL,
    salary DECIMAL(8, 2) NOT NULL,
    department_id INT(11) UNSIGNED NOT NULL,
    location_id INT(11) UNSIGNED NOT NULL,
    country_id CHAR(2) NOT NULL,
    region_id INT(11) UNSIGNED NOT NULL,
    PRIMARY KEY(track_employee_id), 
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
    FOREIGN KEY (department_id) REFERENCES departments(department_id),
    FOREIGN KEY (location_id) REFERENCES locations(location_id),
    FOREIGN KEY (country_id) REFERENCES countries(country_id),
    FOREIGN KEY (region_id) REFERENCES regions(region_id)
);