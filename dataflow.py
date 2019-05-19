"""dataflow.py is a Dataflow pipeline which reads a file and writes its
contents to a BigQuery table.
This example does not do any transformation on the data.

"""

from __future__ import absolute_import
import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions


class DataIngestion:
    """A helper class which contains the logic to translate the file into
    a format BigQuery will accept."""

    def parse_method(self, string_input):
        """This method translates a single line of comma separated values to a
        dictionary which can be loaded into BigQuery.

        Args:
            string_input: A comma separated list of values in the form of
            state_abbreviation,gender,year,name,count_of_babies,dataset_created_date
                example string_input: KS,F,1923,Dorothy,654,11/28/2016

        Returns:
            A dict mapping BigQuery column names as keys to the corresponding value
            parsed from string_input.  In this example, the data is not transformed, and
            remains in the same format as the CSV.  There are no date format transformations.
            example output:
              {'state': 'KS',
               'gender': 'F',
               'year': '1923', <- Keep the format of the "date" as a simple integer, with no transformations.
               'name': 'Dorothy',
               'number': '654',
               'created_date': '11/28/2016',
               'hashcode': '<>'
               }
         """
        # Strip out return characters and quote characters.
        values = re.split(",",
                          re.sub('\r\n', '', re.sub(u'"', '', string_input)))

        row = dict( zip(('state', 'gender', 'year', 'name', 'number',
                         'created_date'),
                values))

        return row


def run(argv=None):
    """The main function which creates the pipeline and runs it."""
    parser = argparse.ArgumentParser()
    # Here we add some specific command line arguments we expect.
    # Specifically we have the input file to load and the output table to
    # This is the final stage of the pipeline, where we define the destination
    # of the data.  In this case we are writing to BigQuery.
    parser.add_argument(
        '--input', dest='input', required=False,
        help='Input file to read.  This can be a local file or '
             'a file in a Google Storage Bucket.',
        # This example file contains a total of only 10 lines.
        # Useful for developing on a small set of data
        default='gs://python-dataflow-example/data_files/head_usa_names.csv')
    # This defaults to the lake dataset in your bigquery project.  You'll have
    # to create the lake dataset yourself using this command:
    # bq mk lake

    parser.add_argument('--output', dest='output', required=False,
                        help='Output BQ table to write results to.',
                        default='lake.usa_names')
    parser.add_argument('--project', dest='project', required=True,
                        help='Define the Google Cloud project.',
                        default=None)
    parser.add_argument('--jobname', dest='jobname', required=True,
                        help='Define the job name.',
                        default=None)
    parser.add_argument('--staging_location', dest='staging_location',
                        required=True,
                        help='Define the staging location.It can also be a bucket location',
                        default=None)
    parser.add_argument('--temp_location', dest='temp_location', required=True,
                        help='Define the temp location. It can also be a bucket location',
                        default=None)
    parser.add_argument('--runner', dest='runner', required=False,
                        default='DataflowRunner')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    # DataIngestion is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.
    data_ingestion = DataIngestion()

    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line.  This includes information including where Dataflow should
    # store temp files, and what the project id is
    options = PipelineOptions(pipeline_args)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = known_args.project
    google_cloud_options.job_name = known_args.jobname
    google_cloud_options.staging_location = known_args.staging_location
    google_cloud_options.temp_location = known_args.temp_location
    options.view_as(StandardOptions).runner = known_args.runner
    p = beam.Pipeline(options=options)

    (p
     # Read the file.  This is the source of the pipeline.  All further
     # processing starts with lines read from the file.  We use the input
     # argument from the command line.  We also skip the first line which is a
     # header row.
     | 'Read from a File' >> beam.io.ReadFromText(known_args.input,
                                                  skip_header_lines=1)
     # This stage of the pipeline translates from a CSV file single row
     # input as a string, to a dictionary object consumable by BigQuery.
     # It refers to a function we have written.  This function will
     # be run in parallel on different workers using input from the
     # previous stage of the pipeline.
     | 'String To BigQuery Row' >> beam.Map(lambda s:
                                            data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.Write(
        beam.io.WriteToBigQuery(
            # The table name is a required argument for the BigQuery sink.
            # In this case we use the value passed in from the command line.
            known_args.output,
            # Here we use the simplest way of defining a schema:
            # fieldName:fieldType
            schema='state:STRING,gender:STRING,year:STRING,name:STRING,'
                   'number:STRING,created_date:STRING',
            # Creates the table in BigQuery if it does not yet exist.
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            # Deletes all data in the BigQuery table before writing.
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
