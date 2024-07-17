GRANT SELECT, MODIFY, READ_METADATA, CREATE ON SCHEMA hr_db TO hr_team;

GRANT USAGE ON SCHEMA hr_db TO hr_team;

GRANT SELECT ON VIEW hr_db.paris_employee_vw TO 'adam@mycompany.com';

SHOW GRANTS ON SCHEMA hr_db;

SHOW GRANTS ON VIEW hr_db.paris_employee_vw;
