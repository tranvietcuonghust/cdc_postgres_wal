import os
import psycopg2
from psycopg2.extras import LogicalReplicationConnection
import json
import pika

def write_to_file(message, filename="taxi_trip_data_3.json"):
    """
    Appends the message to a JSON file.
    """
    with open(filename, "a") as f:
        f.write(json.dumps(message) + "\n")

def get_last_processed_lsn(filename="last_processed_lsn_3.txt"):
    """
    Retrieves the last processed LSN from a file.
    """
    if not os.path.exists(filename):
        return None
    with open(filename, "r") as f:
        return int(f.read().strip())

def update_last_processed_lsn(lsn, filename="last_processed_lsn_3.txt"):
    """
    Updates the file with the last processed LSN.
    """
    with open(filename, "w") as f:
        f.write(lsn)

def main():
    conn = psycopg2.connect(
        database="taxi_trip",
        user='postgres',
        password='postgres',
        host='localhost',
        port='5435',
        connection_factory=LogicalReplicationConnection
    )

    cur = conn.cursor()
    options = {
        'include-timestamp': True,
        'add-tables': 'public.taxi_trip_records',
        'actions': 'insert',
        'pretty-print': True,
    }

    slot_name = 'taxi_trip_slot_5'

    # Attempt to fetch the last processed LSN
    last_processed_lsn = get_last_processed_lsn()
    # last_processed_lsn= 29170248

    if last_processed_lsn:

        try:
            cur.start_replication(slot_name=slot_name, decode=False, options=options, start_lsn = last_processed_lsn)
        except psycopg2.ProgrammingError:
            cur.create_replication_slot(slot_name, output_plugin='wal2json')
            cur.start_replication(slot_name=slot_name, decode=False, options=options, start_lsn = last_processed_lsn)
    else:
        try:
            cur.start_replication(slot_name=slot_name, decode=False, options=options)
        except psycopg2.ProgrammingError:
            cur.create_replication_slot(slot_name, output_plugin='wal2json')
            cur.start_replication(slot_name=slot_name, decode=False, options=options)


    class Decoder(object):
        def __init__(self):
            self.first_message = True

        def __call__(self, msg):
            print("Original Message:\n", msg.payload)
            message = msg.payload
            formatted_message = json.loads(message.decode())
            
            if formatted_message is not None and not self.first_message:  # Check to skip the first message
                write_to_file(formatted_message)
            else:
                self.first_message = False  # Reset the flag after skipping the first message
            # Update the last processed LSN after processing the message
            update_last_processed_lsn(str(msg.wal_end))

            # Send feedback to the server to confirm message processing
            msg.cursor.send_feedback(flush_lsn=msg.wal_end)

    decoder = Decoder()


    def start_stream():
        cur.consume_stream(decoder)

    try:
        start_stream()
    except Exception as e:
        print(e)
        decoder.connection.close()

if __name__ == '__main__':
    main()
