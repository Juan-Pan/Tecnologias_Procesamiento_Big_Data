import sys
import os
import boto3
import pandas as pd
from io import StringIO

# configuracion
bucket_name = "juanpan"
region = "eu-south-2"
perfil_aws = "juanda"

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
input_file = os.path.join(base_dir, "data", "avax_history.csv")

def init_s3():
    try:
        session = boto3.Session(profile_name=perfil_aws)
        return session.client('s3', region_name=region)
    except Exception as e:
        print(f"error de autenticacion: {e}")
        sys.exit(1)

def main():
    if not os.path.exists(input_file):
        print("error: no encuentro el archivo de entrada")
        return

    print("leyendo archivo maestro...")
    df = pd.read_csv(input_file)
    
    col_fecha = 'datetime' 
    if col_fecha not in df.columns:
        print("error: no veo la columna datetime")
        return
        
    df[col_fecha] = pd.to_datetime(df[col_fecha])

    s3 = init_s3()

    print(f"subiendo datos al bucket: {bucket_name}...")

    # agrupar por año y mes
    grupos = df.groupby([df[col_fecha].dt.year, df[col_fecha].dt.month])

    for (anio, mes), datos_mes in grupos:
        # ordenar los datos por dia para garantizar el orden yyyy/mm/dd dentro del csv
        datos_mes = datos_mes.sort_values(by=col_fecha)

        mes_str = f"{mes:02d}"
        
        # esto crea: carpeta año -> archivo del mes
        # ejemplo: 2021/avax_2021_01.csv
        key_s3 = f"{anio}/avax_{anio}_{mes_str}.csv"
        
        csv_buffer = StringIO()
        datos_mes.to_csv(csv_buffer, index=False)
        
        print(f"subiendo: {key_s3} ({len(datos_mes)} filas)")
        
        try:
            s3.put_object(
                Bucket=bucket_name,
                Key=key_s3,
                Body=csv_buffer.getvalue()
            )
        except Exception as e:
            print(f"error subiendo {key_s3}: {e}")

    print("carga finalizada")

if __name__ == "__main__":
    main()