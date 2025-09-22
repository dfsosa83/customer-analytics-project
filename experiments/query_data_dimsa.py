from snowflake_connect import create_session
import pandas as pd

def get_all_tables_data():
    schemas_tables = {
        'PROD.DIMSA_CUSCA_ADMIN': ['SIS_EMISIONES','SIS_EMISIONES_CATEGORIAS'],
        'PROD.DIMSA_CUSCA_PUESTO': ['CUS_SALDOS','CUS_SALDOS_MANANA','CUS_SALDOS_HOY','POR_MAESTRO','POR_COMPRAS'],
        'PROD.DIMSA_FONDOS': ['VAL_VECTOR_HIS']
    }


    session = create_session()
    if session is None:
        print("Failed to create Snowflake session.")
        return None

    all_data = {}
    
    try:
        for qualified_schema, tables in schemas_tables.items():
            for table in tables:
                # Use fully qualified table name
                qualified_table = f"{qualified_schema}.{table}"
                print(f"Fetching data from {qualified_table}...")
                
                try:
                    # Use Snowpark's table function
                    snow_df = session.table(qualified_table).limit(1000000)
                    pandas_df = snow_df.to_pandas()  # Process in chunks

                    
                    # Save to CSV
                    csv_filename = f"{qualified_schema.split('.')[-1]}_{table}.csv"
                    pandas_df.to_csv(csv_filename, index=False)
                    print(f"✅ Saved {csv_filename} with {len(pandas_df)} rows")
                    
                    all_data[qualified_table] = pandas_df
                    
                except Exception as e:
                    print(f"❌ Error processing {qualified_table}: {str(e)}")
                    all_data[qualified_table] = None

        return all_data

    except Exception as e:
        print(f"⚠️ Global error: {str(e)}")
        return None

    finally:
        session.close()

if __name__ == '__main__':
    data = get_all_tables_data()
    if data:
        print("\nExtraction Summary:")
        for key, df in data.items():
            status = "SUCCESS" if df is not None else "FAILED"
            print(f"- {key}: {status}")
    else:
        print("No data extracted")








# 'PROD.DIMSA_CUSCA_ADMIN': [
#                                'AUD_MOVIMIENTOS',
#                                'AUD_PARAM_MOVIMIENTOS','CLI_EMPRESAS','CLI_GRUPOS_ECONOMICOS','CLI_PERSONAS','CLI_TIPOS_CUENTAS','CLI_TIPOS_IDENTIFICACIONES','CLI_TIPOS_RELACIONES',
#                                'SIS_ACTIVIDADES_ECONOMICAS','SIS_ASISTENTES','SIS_BOLSAS','SIS_CATEGORIAS_DETALLE','SIS_EMISIONES','SIS_EMISIONES_CATEGORIAS','SIS_INSTRUMENTOS',
#                                'SIS_TIPOS_CAMBIO','SIS_TIPOS_EMISIONES','SIS_CATEGORIAS','SIS_MONEDAS','SIS_PAISES','SIS_TASAS_VALORES','SIS_CUSTODIOS','SIS_EMISORES','SIS_TASAS',],  # Replace with your real table names
#        'PROD.DIMSA_CUSCA_PUESTO': [
#                                'BAN_BANCOS','BAN_CIERRES_DIARIOS','BAN_CUENTAS','BAN_TIPOS_MOVIMIENTOS','CLI_AUTORIZADOS','CLI_CLIENTES_PREFERENCIAS_ENVIO','CLI_CUENTAS','CLI_CUENTAS_BANCARIAS',
#                                'CLI_CUENTAS_BENEFICIOS','CLI_CUENTAS_BURSATILES','CLI_CUENTAS_FATCA','CLI_CUENTAS_GRUPOS_ECONOMICOS','CLI_PREFERENCIAS_ENVIOS','CLI_TITULARES',
#                                'CON_CIERRES_DIARIOS','CON_TIPOS_ASIENTOS','CON_TIPOS_CUENTAS','CUS_COMPROMISOS','CUS_MOVIMIENTOS','CUS_SALDOS','CUS_TIPOS_COMPROMISOS',
#                                'CUS_TIPOS_MOVIMIENTOS','OPE_BOLETAS','OPE_BOLETAS_ASIGNACION','OPE_BOLETAS_ASIGNACION_OTROS','OPE_ORDENES','OPE_OTRAS_COMISIONES',
#                                'OPE_OTRAS_COMISIONES_CONCEPTOS','POR_RECOMPRAS_PAS_GARANTIA','POR_RECOMPRAS_PAS_INTERES','POR_VALORACION_HIS','POR_VENTAS_DETALLES',
#                                'VAL_VECTOR_HIST','CUS_RECOMPRAS_ACTIVAS','CUS_SALDOS_MANANA','BAN_MOVIMIENTOS','CLI_CUENTAS_CATEGORIAS','CLI_CUENTAS_UBICACIONES',
#                                'CON_ASIENTOS','CON_COMPROBANTES','CON_TIPOS_CAMBIO','OPE_CIERRES_DIARIOS','OPE_PUESTOS','POR_COMPRAS','CLI_CUENTAS_OTROS_DATOS','CON_CATALOGO',
#                                'CUS_SALDOS_HOY','CUS_TIPOS_CUSTODIAS','CON_ASIENTOS_DETALLES','CON_COMPANIAS','CUS_CIERRES_DIARIOS','CUS_TIPOS_SALDOS','CUS_COMPROMISOS_DETALLE',
#                                'OPE_BOLETAS_OTROS','OPE_CENTRALES_ANOTACION','OPE_ORDENES_OTROS','OPE_TIPOS_MOVIMIENTOS','OPE_TIPOS_OPERACIONES','POR_CIERRES_DIARIOS',
#                                'POR_COMPRAS_ASIGNACION','POR_RECOMPRAS_PASIVAS','POR_VENTAS','VAL_VALORACION','OPE_MOVIMIENTOS','OPE_ORDENES_DETALLE','OPE_ORDENES_TITULOS','POR_MAESTRO',
#                                'SIS_EJECUTIVOS','POR_PORTAFOLIOS',],
#        'PROD.DIMSA_FONDOS': [
#                                'ACC_ACCIONISTA','ACC_DIVIDENDOS','ACC_MOVIMIENTOS','BAN_BANCOS','BAN_CIERRES_DIARIOS','BAN_CUENTAS','BAN_MOVIMIENTOS','CLI_CLASIFICACION_SUGEVAL',
#                                'CON_ASIENTOS','CON_ASIENTOS_DETALLES','CON_CATALOGO','CON_CIERRES_DIARIOS','CON_TIPOS_ASIENTOS','CON_TIPOS_CAMBIO','CON_TIPOS_CUENTAS','EXP_INGRE_FONDOS',
#                                'EXP_INGRE_FONDOS_CONTA','EXP_INGRE_FONDOS_FORMULA','EXP_INGRE_TIPOS','PFI_AUTORIZADOS','PFI_FONDOS','PFI_ORDENES','PFI_SOLICITUD_RETIRO_DET','PFI_TIPOS_CUENTAS',
#                                'PFI_TITULARES','PFI_VALOR_PARTICIPACION','POR_CLASIFICACION_INVERSIONES','POR_VENTAS','EXP_RESULTADO_SQL','PFI_CUENTAS_BANCARIAS','PFI_CUENTAS_UBICACIONES',
#                                'PFI_RETIRO_FOCAL_TRASLADADO','PFI_SOLICITUD_RETIRO','POR_COMPRAS_ASIGNACION_DESTINO','POR_RECOMPRAS_PASIVAS','BAN_TIPOS_MOVIMIENTOS','CON_COMPANIAS','PFI_FORMAS_PAGO',
#                                'PFI_RETIRO','PFI_SERIES','PFI_SOCIEDADES','PFI_SOLICITUD_ORDEN','PFI_SOLICITUD_ORDEN_DET','POR_VENTAS_DETALLES','SIS_EJECUTIVOS','VAL_VALORACION','CON_COMPROBANTES',
#                                'PFI_CUENTAS','PFI_INFORMACION_DIARIA','POR_CIERRES_DIARIOS','POR_COMPRAS_ASIGNACION','POR_RECOMPRAS_PAS_GARANTIA','POR_COMPRAS','POR_PORTAFOLIOS',
#                                'POR_TIPOS_MOVIMIENTOS','POR_VALORACION','POR_VALORACION_HIS','VAL_VECTOR_HIST','PFI_COMISION_SALIDA','POR_MAESTRO','POR_RECOMPRAS_PAS_INTERES','PFI_PROCESOS_CIERRE',
#        ]