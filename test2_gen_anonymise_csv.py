import csv
import hashlib
from faker import Faker


def generate_sample_csv(file_name, num_records):
    """
    Generates a sample CSV file with first_name, last_name, address, and date_of_birth.
    """
    # Initialize Faker for generating random names and addresses
    fake = Faker()

    # Open a CSV file for writing
    with open(file_name, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['first_name', 'last_name', 'address', 'date_of_birth'])
        for _ in range(num_records):
            first_name = fake.first_name()
            last_name = fake.last_name()
            address = fake.address().replace("\n", ", ")
            date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=80)
            
            csv_writer.writerow([first_name, last_name, address, date_of_birth])

    print(f"Sample CSV file '{file_name}' generated with {num_records} records.")

def anonymize_text(text):
    """
    Anonymize text by hashing it.
    """
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

def anonymize_csv(input_file, output_file):
    """
    Anonymize the specified columns in the CSV file and save to a new file.
    """
    with open(input_file, 'r', encoding='utf-8') as csvfile_in, \
         open(output_file, 'w', newline='', encoding='utf-8') as csvfile_out:
        
        csv_reader = csv.DictReader(csvfile_in)
        csv_writer = csv.DictWriter(csvfile_out, fieldnames=csv_reader.fieldnames)
        
 
        csv_writer.writeheader()

        for row in csv_reader:
            # Anonymize specific fields
            row['first_name'] = anonymize_text(row['first_name'])
            row['last_name'] = anonymize_text(row['last_name'])
            row['address'] = anonymize_text(row['address'])
            
            # Write the anonymized row to the new CSV
            csv_writer.writerow(row)

    print(f"Anonymized CSV file '{output_file}' generated successfully.")


if __name__ == '__main__':
    sample_file = 'sample_data.csv'
    anonymized_file = 'anonymized_data.csv'
    num_records = 100000  
    generate_sample_csv(sample_file, num_records)

    anonymize_csv(sample_file, anonymized_file)

    print("Data processing complete!")

# Using pyspark 
def anonymize_with_spark(input_file, output_file):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import sha2

    spark = SparkSession.builder \
        .appName("Anonymize CSV") \
        .getOrCreate()


    df = spark.read.csv(input_file, header=True, inferSchema=True)

    df_anonymized = df.withColumn('first_name', sha2(df['first_name'], 256)) \
                      .withColumn('last_name', sha2(df['last_name'], 256)) \
                      .withColumn('address', sha2(df['address'], 256))

    df_anonymized.write.csv(output_file, header=True, mode='overwrite')

    # Stop the Spark session
    spark.stop()

    print(f"Anonymized CSV file with Spark '{output_file}' generated successfully.")
