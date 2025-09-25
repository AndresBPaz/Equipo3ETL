import pandas as pd
import openpyxl
from pathlib import Path

#This function is to extract data from xslx file that we have in data folder
def extract_data_from_xslx(sheet_name=None):
    df = pd.read_excel(sheet_name, engine="openpyxl", header=0)
    return df


#This function is to clean column names of a dataframe
def clean_column_names(df):
    print(df.columns)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("(", "").str.replace(")", "")
    print(df.columns)
    return df

#Function to delete specific columns from a dataframe
def delete_columns(df, columns_to_delete):
    df = df.drop(columns=columns_to_delete)
    return df

#Function to filter rows based on a column value
def filter_rows_by_value(df, column_name, value, comparison_type='equals'):
    if comparison_type == 'equals':
        filtered_df = df[df[column_name] == value]
    elif comparison_type == 'not_equals':
        filtered_df = df[df[column_name] != value]
    elif comparison_type == 'greater_than':
        filtered_df = df[df[column_name] > value]
    elif comparison_type == 'less_than':
        filtered_df = df[df[column_name] < value]
    else:
        raise ValueError("Invalid comparison_type. Use 'equals', 'not_equals', 'greater_than', or 'less_than'.")
    return filtered_df

#Function to delete first n characters from a column
def delete_first_n_characters(df, column_name, n):
    df[column_name] = df[column_name].astype(str).str[n:]
    return df

#Main function to call the other functions
def main():
    df_abastecimientos = extract_data_from_xslx("./Data/abastecimientos_sap.xlsx")
    df_actividades = extract_data_from_xslx("./Data/actividades.xlsx")
    df_insumos = extract_data_from_xslx("./Data/detalle_apuntamiento_insumos.xlsx")
    df_rep_maquinaria = extract_data_from_xslx("./Data/rep_maquinaria.xlsx")
    
    
    #lImpieza la limpieza de los nombres de las columnas
    clean_column_names(df_abastecimientos) 
    clean_column_names(df_actividades)
    clean_column_names(df_insumos)
    clean_column_names(df_rep_maquinaria)
    
    #Eliminar columnas innecesarias para el dataframe de abastecimientos
    innecesary_columns = ['material', 'texto_breve_de_material', 'almacén', 'clase_de_movimiento', 'posición_doc.mat.',
                          'nº_reserva', 'centro_de_coste', 'un.medida_de_entrada']
    for column in innecesary_columns:
        if column in df_abastecimientos.columns:
            df_abastecimientos = delete_columns(df_abastecimientos, [column])
            
    #Con este filtrado, se eliminan tambien los vacios de la columna de equipo
    df_abastecimientos = filter_rows_by_value(df_abastecimientos, 'clase_de_movimiento', 261)
    df_abastecimientos = filter_rows_by_value(df_abastecimientos, 'centro_de_coste', 2000000, 'less_than')
        
    #Eliminar columnas innecesarias para el dataframe de abastecimientos
    innecesary_columns = ['material', 'texto_breve_de_material', 'almacén', 'clase_de_movimiento', 'posición_doc.mat.',
                            'nº_reserva', 'centro_de_coste', 'un.medida_de_entrada']
    
    for column in innecesary_columns:
        if column in df_abastecimientos.columns:
            df_abastecimientos = delete_columns(df_abastecimientos, [column])

    df_abastecimientos.rename(columns={'fe.contabilización': 'fecha'}, inplace=True)
    df_abastecimientos.rename(columns={'orden': 'equipo'}, inplace=True)
    df_abastecimientos.rename(columns={'ctd.en_um_entrada': 'galones'}, inplace=True)
    
    df_abastecimientos = delete_first_n_characters(df_abastecimientos, 'equipo', 3)
    
if __name__ == "__main__":
    main()