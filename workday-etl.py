from datetime import date, datetime
import configparser, os, sys
import pandas as pd

timeoff            =        __import__('timeoff-etl')
workday_ihsm       =        __import__('workday_ihsm_osttra')
workday_compliance =        __import__('workdaycompliance-etl')

SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(SCRIPT_DIR)
sys.path.append("{}/modeljson".format(SCRIPT_DIR))
from modeljson.generate_cdm import cdm_structure_generator

class WorkdayETL:
    def __init__(self):
        current_time = datetime.now().strftime("%X")
        crontimeinhhmmss = current_time.replace(':', '')
        self.timeoff_etl = timeoff.ETL(crontimeinhhmmss)
        self.workday_ihsm_osttra = workday_ihsm.ETL(crontimeinhhmmss)
        self.workday_compliance = workday_compliance.ETL(crontimeinhhmmss)
        self.azure_blob_params, self.model_params = self.azure_credentials()

    def run(self):
        self.model()
        # timeoff etl
        df = self.timeoff_etl.run()
        self.add_entity(df, 'timeoffdashboards')
        # workday ihsm osttra
        df = self.workday_ihsm_osttra.run()
        self.add_entity(df, 'IHSM+OSTTRA_Workday')
        self.upload_model()

        # workday compliance
        self.run_compliance()

    def run_compliance(self):
        """Workday compliance for 10th of every month"""
        self.model()
        if date.today().day == 10:
            df = self.workday_compliance.run()
            self.add_entity(df, 'dashboards')
        self.upload_model()


    def azure_credentials(self):
        """Read the credential for azure from config"""
        config = configparser.ConfigParser()
        config.read('/opt/ihsm/configs/workdaycompliance.ini')
        # config.read('config.ini')
        account = config['job_config']['destination_env']
        azure_blob_params = config[f'azure_blob_{account}']
        model_params = config[f'modeljson_{account}']
        return azure_blob_params, model_params

    def model(self):
        model_dir = "workday/dashboards/"
        model_name = "Workday"
        model_desc = f"Workday metadata about components"
        upload_container = "modeljsons"
        try:
            self.model = cdm_structure_generator(name=model_name, description=model_desc)
            print("Successfully generated structure Name: {0}, Description: {1}".format(model_name, model_desc))
            self.model.connect_to_cdmfolder(model_dir, self.model_params['storage_account_url'], self.model_params["storage_account_key"] , upload_container)
            print("Successfully connected to model directory: ", model_dir)
            self.model.set_model_modified_now()
        except Exception as e:
            print(f'Exception happened while connecting to modeljson folder : {e.args}')
            self.logger.exception(f'Exception happened while connecting to modeljson folder : {e.args}')

    def upload_model(self):
        model_dir = "workday/dashboards/"
        upload_container = "modeljsons"
        try:
            self.model.push_model_to_cdmfolder(overwrite=True, dir=model_dir, account_url=self.model_params['storage_account_url'], key=self.model_params["storage_account_key"], container_name=upload_container)
        except Exception as e:
            self.logger.exception(f'Exception happened in uploading model.json : {e.args}')
            print(f'Exception happened in uploading model.json : {e.args}')
        self.logger.info("Completed Creating Model.json in folder")
        print("Completed Creating Model.json in folder")

    def add_entity(self, csv_filepath, component):
        entity_desc = f"Workday metadata about {component}"
        seperator = ","
        csv_file = csv_filepath.split('/')[-1]
        partition_name = f"dashboards/{component}"
        data_url = f"{self.azure_blob_conn_params['storage_account_url']}workday/{component}/{csv_file}"
        print('Started Creating Model.json')
        self.logger.info('Started Creating Model.json')
        csv_data = pd.read_csv(csv_filepath, engine='python', error_bad_lines=False)

        # try
        self.model.entities.generate_entity_from_dataframe(component, entity_desc, csv_data, partition_name, data_url.replace("dfs","blob"))
        print("Successfully generated entity from dataframe")
        self.logger.info("Successfully generated entity from dataframe")

        self.model.get_entity(component).get_partitions().set_fileFormatSettings(partition_name, headers=True, delimiter=seperator)
        print("Successfully generated partitions")
        self.logger.info("Successfully generated partitions")

        self.model.to_dict()
        # except Exception as e: 
        print(f"Failed to generate model.json : {e.args}")
        self.logger.exception(f"Failed to generate model.json : {e.args}")


def lambda_handler():
    print("start")
    workday = WorkdayETL()
    workday.run()
    print("Inserted the records into the table\n end")

lambda_handler()
