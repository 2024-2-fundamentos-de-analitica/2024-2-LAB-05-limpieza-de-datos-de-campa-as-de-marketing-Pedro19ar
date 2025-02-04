"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel



"""
En esta tarea se le pide que limpie los datos de una campaña de
marketing realizada por un banco, la cual tiene como fin la
recolección de datos de clientes para ofrecerls un préstamo.

La información recolectada se encuentra en la carpeta
files/input/ en varios archivos csv.zip comprimidos para ahorrar
espacio en disco.

Usted debe procesar directamente los archivos comprimidos (sin
descomprimirlos). Se desea partir la data en tres archivos csv
(sin comprimir): client.csv, campaign.csv y economics.csv.
Cada archivo debe tener las columnas indicadas.

Los tres archivos generados se almacenarán en la carpeta files/output/.

client.csv:
- client_id
- age
- job: se debe cambiar el "." por "" y el "-" por "_"
- marital
- education: se debe cambiar "." por "_" y "unknown" por pd.NA
- credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
- mortage: convertir a "yes" a 1 y cualquier otro valor a 0

campaign.csv:
- client_id
- number_contacts
- contact_duration
- previous_campaing_contacts
- previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
- campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
- last_contact_day: crear un valor con el formato "YYYY-MM-DD",
    combinando los campos "day" y "month" con el año 2022.

economics.csv:
- client_id
- const_price_idx
- eurobor_three_months



"""
import pandas as pd
import zipfile
import os
from io import TextIOWrapper

def clean_campaign_data():
    input_folder = "files/input/"
    output_folder = "files/output/"
    os.makedirs(output_folder, exist_ok=True)
    
    client_data = []
    campaign_data = []
    economics_data = []
    
    zip_files = [
        "files/input/bank-marketing-campaing-0.csv.zip",
        "files/input/bank-marketing-campaing-1.csv.zip",
        "files/input/bank-marketing-campaing-2.csv.zip",
        "files/input/bank-marketing-campaing-3.csv.zip",
        "files/input/bank-marketing-campaing-4.csv.zip",
        "files/input/bank-marketing-campaing-5.csv.zip",
        "files/input/bank-marketing-campaing-6.csv.zip",
        "files/input/bank-marketing-campaing-7.csv.zip",
        "files/input/bank-marketing-campaing-8.csv.zip",
        "files/input/bank-marketing-campaing-9.csv.zip"
    ]
    
    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, 'r') as z:
            for filename in z.namelist():
                with z.open(filename) as file:
                    df = pd.read_csv(TextIOWrapper(file, 'utf-8'), sep=',')
                    
                    if "mortage" in df.columns:
                        df.rename(columns={"mortage": "mortgage"}, inplace=True)
                    elif "mortgage" not in df.columns:
                        df["mortgage"] = "unknown"
                    
                    if "previous_campaing_contacts" in df.columns:
                        df.rename(columns={"previous_campaing_contacts": "previous_campaign_contacts"}, inplace=True)
                    elif "previous_campaign_contacts" not in df.columns:
                        df["previous_campaign_contacts"] = 0
                    
                    if "const_price_idx" in df.columns:
                        df.rename(columns={"const_price_idx": "cons_price_idx"}, inplace=True)
                    elif "cons_price_idx" not in df.columns:
                        df["cons_price_idx"] = pd.NA
                    
                    if "eurobor_three_months" in df.columns:
                        df.rename(columns={"eurobor_three_months": "euribor_three_months"}, inplace=True)
                    elif "euribor_three_months" not in df.columns:
                        df["euribor_three_months"] = pd.NA
                    
                    client_df = df[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]].copy()
                    client_df["job"] = client_df["job"].str.replace("\\.", "", regex=True).str.replace("-", "_", regex=True)
                    client_df["education"] = client_df["education"].str.replace("\\.", "_", regex=True).replace("unknown", pd.NA)
                    client_df["credit_default"] = client_df["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
                    client_df["mortgage"] = client_df["mortgage"].apply(lambda x: 1 if x == "yes" else (0 if x == "no" else pd.NA))
                    client_data.append(client_df)
                    
                    campaign_df = df[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "day", "month"]].copy()
                    campaign_df["previous_outcome"] = campaign_df["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
                    campaign_df["campaign_outcome"] = campaign_df["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
                    campaign_df["last_contact_date"] = pd.to_datetime(
                        campaign_df["day"].astype(str) + "-" + campaign_df["month"] + "-2022", format="%d-%b-%Y")
                    campaign_df.drop(columns=["day", "month"], inplace=True)
                    campaign_data.append(campaign_df)
                    
                    economics_df = df[["client_id", "cons_price_idx", "euribor_three_months"]].copy()
                    economics_data.append(economics_df)
                    
    pd.concat(client_data).to_csv(os.path.join(output_folder, "client.csv"), index=False)
    pd.concat(campaign_data).to_csv(os.path.join(output_folder, "campaign.csv"), index=False)
    pd.concat(economics_data).to_csv(os.path.join(output_folder, "economics.csv"), index=False)

if __name__ == "__main__":
    clean_campaign_data()









