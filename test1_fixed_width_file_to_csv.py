import csv

def parse_spec_file(spec_file):
    """
    Reads the specification file to get the names and lengths of each field.
    Returns a list of field names and a list of field lengths.
    """
    fields = []  # List to store field names
    field_lengths = []  # List to store field lengths

    # read the file
    with open(spec_file, 'r') as file:
        specs = file.readlines()  # Read all lines

    # looping for spec file
    for spec in specs:
        field_name, field_length = spec.strip().split()
        fields.append(field_name)
        field_lengths.append(int(field_length))
    return fields, field_lengths

def parse_fixed_width_file(fixed_width_file, fields, field_lengths, output_file):
    """
    Parses the fixed-width file using the specified field lengths.
    Writes the parsed data to a CSV file.
    """
    # Opening the fixed-width file in read mode 
    with open(fixed_width_file, 'r', encoding='utf-8') as fw_file, \
         open(output_file, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)  # Create a CSV writer object
        csv_writer.writerow(fields)
        for line in fw_file:
            record = []  
            start = 0 
            for length in field_lengths:
                field_value = line[start:start + length].strip()
                record.append(field_value)

                # Move the start position to the next field
                start += length

            # Write the records to  CSV file
            csv_writer.writerow(record)

def main():
    # Define the paths for the spec file, fixed-width file, and output CSV file
    spec_file = 'specs.txt'
    fixed_width_file = 'fixed_width_file.txt'  # Path to your fixed-width file
    output_file = 'path/output.csv'    # Path for the output CSV file

    # Parse the spec file to get field names and lengths
    fields, field_lengths = parse_spec_file(spec_file)

    # Parse the fixed-width file and write the data to a CSV file
    parse_fixed_width_file(fixed_width_file, fields, field_lengths, output_file)

    print(f"Hurray..!, CSV file '{output_file}' generated successfully!")

if __name__ == '__main__':
    main()
