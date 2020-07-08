
import snowflake.connector as sf

def db_cursor():

    # Create Cursor:
    ctx = sf.connect(
        #host='xxx',
        account='xxx',
        user='xxx',
        password='xxx',
        database='xxx',
        schema='xxx'
    )
    cs = ctx.cursor()
    return ctx, cs


def db_close(cursor):
    cursor.close()

def validate_con():
    # Gets the version
    ctx = snowflake.connector.connect(
        # host='xxx',
        account='xxx',
        user='xxx',
        password='xxx',
        database='xxx',
        warehouse='xxx'
    )
    cs = ctx.cursor()

    try:
        cs.execute("SELECT current_version()")
        one_row = cs.fetchone()
        print(one_row[0])
    finally:
        cs.close()
    ctx.close()
