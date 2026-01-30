import sys
import os
import time
import pandas as pd

# 1. configuracion de rutas para poder importar desde src/utils
# esto permite que python encuentre tu archivo tradingview.py que esta en otra carpeta
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from src.utils.tradingview import TradingViewData, Interval

def descargar_avax():
    print("HU-1: iniciando descarga de avaxusd desde binance...")
    
    # instanciar la clase que copiaste
    tv = TradingViewData()
    
    # parametros segun el pdf (exchange recomendado: binance)
    symbol = "AVAXUSD"
    exchange = "BINANCE"
    
    # nota: n_bars=1500 equivale a unos 4 anos de datos diarios aprox.
    print(f"conectando a tradingview ({exchange}:{symbol})...")
    
    try:
        df = tv.get_hist(
            symbol=symbol,
            exchange=exchange,
            interval=Interval.daily,
            n_bars=1500
        )
        
        # verificacion
        if df is not None and not df.empty:
            print("\nDatos recibidos!")
            print(f"Total registros: {len(df)}")
            print("Primeras 5 filas:")
            print(df.head())
            
            # guardar en la carpeta data/ (que esta ignorada por git)
            ruta_guardado = os.path.join(project_root, 'data', 'avax_history.csv')
            
            # crear carpeta data si no existe
            os.makedirs(os.path.dirname(ruta_guardado), exist_ok=True)
            
            df.to_csv(ruta_guardado)
            print(f"\nArchivo guardado en: {ruta_guardado}")
            print("Historia de usuario 1 completada!")
        else:
            print("Error: el dataframe llego vacio. Revisa tu conexion.")
            
    except Exception as e:
        print(f"Error critico: {e}")

if __name__ == "__main__":
    descargar_avax()