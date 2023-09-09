def get_data_from_mongo():
    import pymongo
    import pandas as pd

    data = "mongodb+srv://parzivala24:pradyumna@cluster0.wiaxsct.mongodb.net/?retryWrites=true&w=majority"

    client = pymongo.MongoClient(data)

    db = client["mydatabase"]

    col = db["advertisement"]

    all_documents = col.find()

    document_list = list(all_documents)

    df = pd.DataFrame(document_list)

    # Convert the "Timestamp" column to datetime if it's not already
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])

    # Format the "Timestamp" column as per your desired format
    df["Timestamp"] = df["Timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Set display options to show the full timestamp
    pd.set_option('display.max_colwidth', None)

    csv_file_path = "/opt/airflow/output.csv"
    df.to_csv(csv_file_path, index=False)
get_data_from_mongo()