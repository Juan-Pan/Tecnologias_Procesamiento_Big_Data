import boto3
import time
import sys

# configuracion 
bucket_name = "juanpan"
region = "eu-south-2"
perfil_aws = "juanda"

# nombre imat3b09
database_name = "trade_data_imat3b09"

# nombre del crawler
crawler_name = "crawler_avax_hist"

# arn de tu rol
role_arn = "arn:aws:iam::585768141030:role/service-role/AWSGlueServiceRole-crawlertest"

def init_glue():
    try:
        session = boto3.Session(profile_name=perfil_aws)
        return session.client('glue', region_name=region)
    except Exception as e:
        print(f"error de autenticacion: {e}")
        sys.exit(1)

def create_database(glue):
    print(f"1. revisando base de datos '{database_name}'")
    try:
        glue.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': 'base de datos para sprint2 avalanche grupo imat3b09'
            }
        )
        print("   - base de datos creada.")
    except glue.exceptions.AlreadyExistsException:
        print("   - la base de datos ya existia.")

def create_crawler(glue):
    print(f"2. configurando crawler '{crawler_name}'")
    
    targets = {
        'S3Targets': [
            {'Path': f's3://{bucket_name}/'}
        ]
    }
    
    prefix = "avax_" 
    
    try:
        glue.create_crawler(
            Name=crawler_name,
            Role=role_arn,
            DatabaseName=database_name, # se apunta a la nueva base de datos trade_data_imat3b09
            Targets=targets,
            TablePrefix=prefix,
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            }
        )
        print("   - crawler creado exitosamente.")
    except glue.exceptions.AlreadyExistsException:
        print("   - el crawler ya existia. actualizando para que apunte a la nueva bbdd...")
       
        glue.update_crawler(
            Name=crawler_name,
            Role=role_arn,
            DatabaseName=database_name,
            Targets=targets,
            TablePrefix=prefix
        )

def start_crawler(glue):
    print(f"3. iniciando ejecucion del crawler...")
    try:
        glue.start_crawler(Name=crawler_name)
        print("   - crawler arrancado.")
    except Exception as e:
        print(f"   - error al iniciar: {e}")

def main():
    glue = init_glue()
    create_database(glue)
    create_crawler(glue)
    start_crawler(glue)

if __name__ == "__main__":
    main()