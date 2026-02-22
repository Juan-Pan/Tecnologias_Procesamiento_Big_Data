import boto3
import sys
import time

#configuracion
bucket_name = "juanpan"
region = "eu-south-2"
perfil_aws = "juanda"
database_name = "trade_data_imat3b09" # La misma base de datos de antes

# crawler y tabla
crawler_name = "crawler_gold_kpi"
target_path = f"s3://{bucket_name}/gold/" 
table_prefix = "kpi_" 

# rol de glue
role_arn = "arn:aws:iam::585768141030:role/service-role/AWSGlueServiceRole-crawlertest"

def init_glue():
    try:
        session = boto3.Session(profile_name=perfil_aws)
        return session.client('glue', region_name=region)
    except Exception as e:
        print(f"Error de autenticación: {e}")
        sys.exit(1)

def create_gold_crawler(glue):
    print(f"Configurando crawler '{crawler_name}' para la capa ORO...")
    
    targets = {
        'S3Targets': [
            {'Path': target_path}
        ]
    }
    
    try:
        glue.create_crawler(
            Name=crawler_name,
            Role=role_arn,
            DatabaseName=database_name,
            Targets=targets,
            TablePrefix=table_prefix,
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            }
        )
        print("   - Crawler creado exitosamente.")
    except glue.exceptions.AlreadyExistsException:
        print("   - El crawler ya existía. Actualizando configuración...")
        glue.update_crawler(
            Name=crawler_name,
            Role=role_arn,
            DatabaseName=database_name,
            Targets=targets,
            TablePrefix=table_prefix
        )

def start_crawler(glue):
    print(f"Iniciando ejecución del crawler '{crawler_name}'...")
    try:
        glue.start_crawler(Name=crawler_name)
        print("   - Crawler arrancado. Escaneando carpeta 'gold/'...")
    except Exception as e:
        print(f"   - Error al iniciar: {e}")

def main():
    glue = init_glue()
    # ya esta creada la base de datos, asi que no la creamos de nuevo
    create_gold_crawler(glue)
    start_crawler(glue)
    print("\nProceso finalizado. Espera unos 2 minutos a que termine de escanear.")

if __name__ == "__main__":
    main()