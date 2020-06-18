
import snowflake.connector as sf

def db_cursor():

    # Create Cursor:
    ctx = sf.connect(
        #host='https://solita.eu-central-1.snowflakecomputing.com/console',
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
