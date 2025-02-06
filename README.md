Overview
This Python script automates the migration of data between Odoo databases (source_db → target_db). It efficiently transfers key tables such as res_users, res_partner, res_groups, res_company, and res_groups_users_rel, while ensuring data consistency and integrity.

One of the standout features of this script is its ability to migrate updated user passwords. When a password is changed in source_db, it gets transferred to target_db, allowing users to log in with their latest credentials post-migration.

Key Features
✅ Migrates essential Odoo tables, including users, partners, groups, and companies.
✅ Ensures updated passwords are migrated, maintaining user authentication.
✅ Handles parent-child relationships, resolving references efficiently.
✅ Resets sequences to maintain ID continuity in the target database.
✅ Supports JSONB data type migration for fields like name and description.
✅ Uses batch processing to optimize large data migrations.

Technologies Used
Python – Core scripting language
PostgreSQL – Database used for Odoo
psycopg2 – PostgreSQL adapter for Python
Odoo ORM concepts – Handling relational data
Setup & Usage
Install dependencies:
bash
Copy
Edit
pip install psycopg2
Configure database connections in the script.
Run the migration script:
bash
Copy
Edit
python migrate_data.py
Verify migrated data in the target Odoo instance.
Future Enhancements
🔹 Automating migration logs for better tracking.
🔹 Adding support for more Odoo tables.
🔹 Optimizing query performance for large datasets.

