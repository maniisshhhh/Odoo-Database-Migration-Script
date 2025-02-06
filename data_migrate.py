import psycopg2
import connection
from datetime import datetime
import json  
from psycopg2 import sql



def fix_missing_partners(target_conn, source_conn, res_partner_columns):
    """Create placeholder partners for missing partner_id values in res_users."""
    try:
        cursor = target_conn.cursor()
        source_cursor = source_conn.cursor()

        # Get all missing partner_ids from source res_users
        source_cursor.execute("""
            SELECT DISTINCT partner_id FROM res_users 
            WHERE partner_id IS NOT NULL;
        """)
        source_partner_ids = source_cursor.fetchall()

        # Check which ones are missing in target res_partner
        missing_partners = []
        for (partner_id,) in source_partner_ids:
            cursor.execute("SELECT id FROM res_partner WHERE id = %s", (partner_id,))
            if not cursor.fetchone():
                missing_partners.append(partner_id)

        if not missing_partners:
            print("✅ No missing partners to fix.")
            return

        print(f"⚠ Found {len(missing_partners)} missing partner records. Creating placeholders...")

        # Insert placeholder records with all necessary fields
        for partner_id in missing_partners:
        # Fetch partner details from the source database
            source_cursor.execute(f"SELECT id, name FROM res_partner WHERE id = %s", (partner_id,))
            partner_data = source_cursor.fetchone()

            if partner_data:
                partner_id, partner_name = partner_data  # Unpack values
                cursor.execute("""
                    INSERT INTO res_partner (id, name) VALUES (%s, %s)
                    ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;
                """, (partner_id, partner_name))  # Ensure name is properly updated
            else:
                print(f"⚠️ Partner with ID {partner_id} not found in source database. Skipping.")
                continue


        target_conn.commit()
        print("✅ Missing partners created.")

    except Exception as e:
        print(f"❌ Error fixing missing partners: {e}")
        target_conn.rollback()
    finally:
        cursor.close()


def migrate_table(source_conn, target_conn, table_name, columns, skip_fkeys=False, batch_size=1000, on_conflict_do_nothing=False, conflict_target=None):
    """Migrate table data with conflict handling."""
    try:
        source_cursor = source_conn.cursor()
        target_cursor = target_conn.cursor()

        query = f"SELECT {', '.join(columns)} FROM {table_name};"
        source_cursor.execute(query)
        all_rows = source_cursor.fetchall()
        
        if not all_rows:
            print(f"⚠ No data found in {table_name}. Skipping...")
            return

        print(f"🔄 Migrating {len(all_rows)} records from {table_name}...")

        if not skip_fkeys:
            target_cursor.execute(f"ALTER TABLE {table_name} DISABLE TRIGGER ALL;")

        column_names = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])

        conflict_clause = ""
        if on_conflict_do_nothing:
            conflict_clause = "ON CONFLICT DO NOTHING"
        elif conflict_target:
            conflict_clause = f"ON CONFLICT ({conflict_target}) DO UPDATE SET {update_set}"
        else:
            conflict_clause = f"ON CONFLICT (id) DO UPDATE SET {update_set}"

        insert_query = f"""
            INSERT INTO {table_name} ({column_names})
            VALUES ({placeholders})
            {conflict_clause};
        """

        for i in range(0, len(all_rows), batch_size):
            batch = all_rows[i:i + batch_size]
            processed_batch = []
            for row in batch:
                processed_row = list(row)
                for idx, col in enumerate(columns):
                    if col in ('write_uid', 'create_uid'):
                        processed_row[idx] = None
                    elif col == 'company_id' and (processed_row[idx] is None or processed_row[idx] == 0):
                        processed_row[idx] = 1
                    elif table_name == 'res_groups' and col in ('name', 'comment') and isinstance(processed_row[idx], dict):
                        processed_row[idx] = json.dumps(processed_row[idx])  # Convert dict to JSON string

                processed_batch.append(tuple(processed_row))

            target_cursor.executemany(insert_query, processed_batch)
            target_conn.commit()
            print(f"✅ Migrated batch {i//batch_size + 1} ({len(batch)} records) from {table_name}")

        target_cursor.execute(f"ALTER TABLE {table_name} ENABLE TRIGGER ALL;")
        print(f"✅ Successfully migrated all records to {table_name}.")
    
    except Exception as e:
        print(f"❌ Error migrating {table_name}: {e}")
        target_conn.rollback()
    finally:
        source_cursor.close()
        target_cursor.close()



def fix_parent_partners(target_conn):
    """Create placeholder records for missing parent partners."""
    try:
        cursor = target_conn.cursor()
        
        # Find missing parent_ids
        cursor.execute("""
            SELECT DISTINCT parent_id FROM res_partner 
            WHERE parent_id IS NOT NULL 
            AND parent_id NOT IN (SELECT id FROM res_partner);
        """)
        missing_parents = cursor.fetchall()
        
        if not missing_parents:
            print("✅ No missing parent partners to fix.")
            return
            
        print(f"⚠ Found {len(missing_parents)} missing parent partners. Creating placeholders...")
        
        # Insert placeholder parent records
        for parent_id in missing_parents:
            cursor.execute("""
                INSERT INTO res_partner 
                (id, name, create_date, write_date, active, company_id)
                VALUES (%s, %s, NOW(), NOW(), TRUE, 1)
                ON CONFLICT DO NOTHING;
            """, (parent_id[0], f"Placeholder Parent Partner {parent_id[0]}"))
            
        target_conn.commit()
        print("✅ Missing parent partners created.")
        
    except Exception as e:
        print(f"❌ Error fixing parent partners: {e}")
        target_conn.rollback()
    finally:
        cursor.close()

def assign_users_to_group(target_conn):
    """Assign all migrated users to the Internal User group."""
    try:
        cursor = target_conn.cursor()

        # Fetch all user IDs from res_users (excluding admin & public user)
        cursor.execute("SELECT id FROM res_users WHERE id > 2;")
        user_ids = cursor.fetchall()

        if not user_ids:
            print("⚠ No users found to assign to groups.")
            return

        # Fetch Internal User group ID safely using XML reference
        cursor.execute("SELECT res_id FROM ir_model_data WHERE module='base' AND name='group_user';")
        group_id = cursor.fetchone()

        if not group_id:
            print("⚠ Could not find Internal User group.")
            return

        insert_values = [(group_id[0], user_id[0]) for user_id in user_ids]

        insert_query = """
            INSERT INTO res_groups_users_rel (gid, uid)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
        """

        cursor.executemany(insert_query, insert_values)
        target_conn.commit()
        print(f"✅ Assigned {len(user_ids)} users to the Internal User group.")

    except Exception as e:
        print(f"❌ Error assigning users to group: {e}")
        target_conn.rollback()
    finally:
        cursor.close()

def reset_sequence(conn, table_name, column_name):
    try:
        cursor = conn.cursor()

        # 1. Get the sequence name
        cursor.execute(f"SELECT pg_get_serial_sequence('{table_name}', '{column_name}');")
        sequence_name = cursor.fetchone()[0]

        if sequence_name:
            # 2. Get the maximum ID value (handle NULLs)
            cursor.execute(f"SELECT COALESCE(MAX(id), 0) FROM {table_name};")  # COALESCE handles NULLs
            max_id = cursor.fetchone()[0]

            # 3. Restart the sequence
            cursor.execute(f"ALTER SEQUENCE {sequence_name} RESTART WITH {max_id + 1};") # Corrected syntax
            conn.commit()
            print(f"✅ Sequence for {table_name}.{column_name} reset.")
        else:
            print(f"⚠ No sequence found for {table_name}.{column_name}. Skipping reset.")

    except Exception as e:
        print(f"❌ Error resetting sequence for {table_name}.{column_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()  


def migrate_res_company_users_rel(source_conn, target_conn):
    """Migrates the res_company_users_rel table."""
    try:
        source_cursor = source_conn.cursor()
        target_cursor = target_conn.cursor()

        # Fetch only records where cid exists in res_company
        source_cursor.execute("""
            SELECT r.* FROM res_company_users_rel r
            JOIN res_company c ON r.cid = c.id;
        """)
        rows = source_cursor.fetchall()

        if not rows:
            print("⚠ No valid data found in source res_company_users_rel. Skipping...")
            return

        print(f"🔄 Migrating {len(rows)} records from res_company_users_rel...")

        # Insert query
        insert_query = """
            INSERT INTO res_company_users_rel (cid, user_id)  
            VALUES (%s, %s)
            ON CONFLICT (cid, user_id) DO NOTHING;
        """

        # Execute insert query
        target_cursor.executemany(insert_query, rows)
        target_conn.commit()
        print("✅ Successfully migrated res_company_users_rel.")

    except Exception as e:
        print(f"❌ Error migrating res_company_users_rel: {e}")
        target_conn.rollback()
    finally:
        if source_cursor:
            source_cursor.close()
        if target_cursor:
            target_cursor.close()



def main():
    source_connection = None # Initialize connection variables
    target_connection = None
    try:
        source_connection = psycopg2.connect(
            host=connection.source_db_host,
            user=connection.source_db_user,
            password=connection.source_db_password,
            database=connection.source_db_name,
            port=connection.source_db_port
        )
        print("✅ Source database connection established.")

        target_connection = psycopg2.connect(
            host=connection.target_db_host,
            user=connection.target_db_user,
            password=connection.target_db_password,
            database=connection.target_db_name,
            port=connection.target_db_port
        )
        print("✅ Target database connection established.")

        target_cursor = target_connection.cursor() #Target cursor for checking res_partner

        # Define columns for migration
        res_partner_columns = [
            "id", "company_id", "name", "title", "parent_id", "user_id", "state_id",
            "country_id", "industry_id", "color", "commercial_partner_id", "create_uid",
            "write_uid", "complete_name", "ref", "lang", "tz", "vat",
            "company_registry", "website", "function", "type", "street", "street2",
            "zip", "city", "email", "phone", "mobile", "commercial_company_name",
            "company_name", "date", "comment", "partner_latitude", "partner_longitude",
            "active", "employee", "is_company", "partner_share"
        ]

        res_users_columns = [
            "id", "company_id", "partner_id", "active", "create_date", "login",
            "password", "action_id", "create_uid", "write_uid", "signature",
            "share", "write_date", "totp_secret", "notification_type",
            "odoobot_state", "odoobot_failed"
        ]

        res_groups_columns = [ 
            "id", "name", "category_id", "color", "create_uid", "write_uid",  # Add other columns as needed
            "comment", "share", "create_date", "write_date"
        ]

        res_company_columns = [  # Define ALL res_company columns
            "id", "name", "partner_id", "currency_id", "sequence", "create_date",
            "parent_path", "parent_id", "paperformat_id", "external_report_layout_id",
            "create_uid", "write_uid", "email", "phone", "mobile", "font",
            "primary_color", "secondary_color", "layout_background", "report_header",
            "report_footer", "company_details", "active", "uses_default_logo", "write_date"
        ]

        res_groups_users_rel_columns = ["gid","uid"]

        # Migration steps
        print("🚀 Starting migration process...")
        
        target_cursor.execute("SELECT COUNT(*) FROM res_partner;")
        count = target_cursor.fetchone()[0]

         # 1. Migrate res_partner (Two-Pass)
        migrate_res_partner_two_pass(source_connection, target_connection, res_partner_columns)
        reset_sequence(target_connection, "res_partner", "id")

        # 2. Fix missing and parent partners (after migrating res_partner)
        fix_missing_partners(target_connection, source_connection, res_partner_columns)  # Corrected call!
        fix_parent_partners(target_connection)

        # 3. Migrate res_users AFTER fixing partners
        migrate_table(source_connection, target_connection, "res_users", res_users_columns, on_conflict_do_nothing=True)

        # 4. Migrate res_users
        migrate_table(source_connection, target_connection, "res_users", res_users_columns, on_conflict_do_nothing=True)

        # 5. Migrate res_company_users_rel
        migrate_res_company_users_rel(source_connection, target_connection)


        #Check if res_partner has any data (and migrate if needed)
        target_cursor.execute("SELECT COUNT(*) FROM res_partner;")
        count = target_cursor.fetchone()[0] 


        migrate_table(source_connection, target_connection, "res_users", res_users_columns, on_conflict_do_nothing=False, conflict_target="id")

        assign_users_to_group(target_connection)
       # Migrate res_groups (with JSON encoding for name/comment)
        migrate_table(source_connection, target_connection, "res_groups", res_groups_columns)

        # Migrate res_groups_users_rel (using ON CONFLICT DO NOTHING)
        migrate_table(source_connection, target_connection, "res_groups_users_rel", ["gid", "uid"], on_conflict_do_nothing=True, conflict_target="gid, uid")  # Corrected!

        print("🎉 Data migration completed successfully!")

        reset_sequence(target_connection, "res_partner", "id")
        reset_sequence(target_connection, "res_users", "id") # Reset for res_users as well
        reset_sequence(target_connection, "res_company", "id")  # Reset for res_company!


    except Exception as e:
        print(f"❌ Database connection error: {e}")
    finally:
        if source_connection: # Check before closing
            source_connection.close()
            print("🔄 Source database connection closed.")
        if target_connection: # Check before closing
            target_connection.close()
            print("🔄 Target database connection closed.")

def migrate_res_partner_two_pass(source_conn, target_conn, columns):
    """Migrate res_partner data in two passes to handle parent_id references properly."""
    source_cursor = source_conn.cursor()
    target_cursor = target_conn.cursor()

    # 1st Pass: Insert Data Without Parent References
    print("🔄 Migrating res_partner (First Pass: Without Parent References)...")
    query = f"SELECT {', '.join(columns)} FROM res_partner;"
    source_cursor.execute(query)
    all_rows = source_cursor.fetchall()

    if not all_rows:
        print("⚠ No data found in res_partner. Skipping...")
        return

    # Disable triggers for performance
    target_cursor.execute("ALTER TABLE res_partner DISABLE TRIGGER ALL;")

    column_names = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))
    
    insert_query = f"""
        INSERT INTO res_partner ({column_names})
        VALUES ({placeholders})
        ON CONFLICT (id) DO NOTHING;
    """

    processed_data = []
    for row in all_rows:
        processed_row = list(row)  # Convert tuple to list
        for idx, col in enumerate(columns):
            if col in ('write_uid', 'create_uid'):
                processed_row[idx] = None
            elif col == 'company_id' and (processed_row[idx] is None or processed_row[idx] == 0):
                processed_row[idx] = 1
            elif col in ('name', 'comment') and isinstance(processed_row[idx], dict):
                processed_row[idx] = json.dumps(processed_row[idx])
        
        processed_data.append(tuple(processed_row))
    
    target_cursor.executemany(insert_query, processed_data)
    target_conn.commit()

    print(f"✅ First pass completed. Inserted {len(processed_data)} records.")

    # 2nd Pass: Fix Parent References (parent_id, commercial_partner_id)
    print("🔄 Updating parent_id and commercial_partner_id in res_partner...")
    update_query = """
        UPDATE res_partner t
        SET parent_id = s.parent_id,
            commercial_partner_id = s.commercial_partner_id
        FROM res_partner s
        WHERE t.id = s.id;
    """
    target_cursor.execute(update_query)
    target_conn.commit()
    print("✅ Parent references updated.")

    # Re-enable triggers
    target_cursor.execute("ALTER TABLE res_partner ENABLE TRIGGER ALL;")

    source_cursor.close()
    target_cursor.close()
    print("🎉 res_partner migration completed successfully.")

def reset_sequence(target_conn, table_name, id_column):
    """Resets the sequence for the given table and ID column."""
    cursor = target_conn.cursor()
    reset_query = f"SELECT setval(pg_get_serial_sequence('{table_name}', '{id_column}'), COALESCE((SELECT MAX({id_column}) FROM {table_name}) + 1, 1), false);"
    cursor.execute(reset_query)
    target_conn.commit()
    cursor.close()
    print(f"🔄 Sequence reset for {table_name}")    

if __name__ == "__main__":
    main()