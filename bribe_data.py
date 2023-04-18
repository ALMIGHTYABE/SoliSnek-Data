import requests
import pandas as pd
import numpy as np
import yaml
import json
import os
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta, TH
from application_logging.logger import logger
import gspread
from gspread_dataframe import set_with_dataframe
from web3 import Web3
from web3.middleware import validation


# Params
params_path = "params.yaml"


def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


config = read_params(params_path)

try:
    # Params Data
    subgraph = config["query"]["subgraph"]
    id_data = config["files"]["id_data"]
    provider_url = config["web3"]["provider_url"]
    bribe_abi = config["web3"]["bribe_abi"]
    epoch_csv = config["files"]["epoch_data"]
    price_api = config["api"]["price_api"]
    bribe_csv = config["files"]["bribe_data"]

    # Pulling Bribe Data
    logger.info("Bribe Data Started")

    ids_df = pd.read_csv(id_data)

    # Get Epoch Timestamp
    todayDate = datetime.utcnow()
    if todayDate.isoweekday() == 4:
        nextThursday = todayDate + relativedelta(weekday=TH(2))
        my_time = datetime.min.time()
        my_datetime = datetime.combine(nextThursday, my_time)
        timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())
        print("Yes, The next Thursday date:", my_datetime, timestamp)
    else:
        nextThursday = todayDate + relativedelta(weekday=TH(0))
        my_time = datetime.min.time()
        my_datetime = datetime.combine(nextThursday, my_time)
        timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())
        print("No, The next Thursday date:", my_datetime, timestamp)

    # Read Epoch Data
    epoch_data = pd.read_csv(epoch_csv)
    epoch = epoch_data[epoch_data["timestamp"] == timestamp]["epoch"].values[0] - 1

    # Pull Bribes Web3
    validation.METHODS_TO_VALIDATE = []
    w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 60}))

    bribes_list = []
    for name, bribe_ca in zip(ids_df["name"], ids_df["bribe_ca"]):
        if bribe_ca == "0x0000000000000000000000000000000000000000":
            pass
        else:
            contract_address = bribe_ca
            contract_instance = w3.eth.contract(address=contract_address, abi=bribe_abi)

            rewardTokens = contract_instance.functions.getRewardTokens().call()

            for reward_addy in rewardTokens:
                rewarddata = contract_instance.functions.tokenTotalSupplyByPeriod(timestamp, reward_addy).call()
                if rewarddata > 0:
                    bribes_list.append({"name": name, "bribes": rewarddata, "address": reward_addy})

    bribe_df = pd.DataFrame(bribes_list)
    bribe_df["address"] = bribe_df["address"].apply(str.lower)

    # Pull Prices
    pricelist = []
    for addy in bribe_df["address"].unique():
        print(addy)
        if addy in ["0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7", "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e", "0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664"]:
            amount = "1000000"
        else:
            amount = "1000000000000000000"
        response = requests.get(f"https://api.paraswap.io/prices/?srcToken={addy}&destToken=0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E&network=43114&partner=solisnek&srcDecimals=18&destDecimals=6&amount={amount}&maxImpact=100", timeout=60)
        price = response.json()["priceRoute"]["srcUSD"]
        decimals = response.json()["priceRoute"]["srcDecimals"]
        pricelist.append({"name": name, "address": addy, "price": price, "decimals": decimals})

    price_df = pd.DataFrame(pricelist, columns=["address", "price", "decimals"])

    # Bribe Amounts
    bribe_df = bribe_df.merge(price_df[["address", "price", "decimals"]], on="address", how="left")
    bribe_df["bribe_amount"] = bribe_df["price"].astype("float") * bribe_df["bribes"]

    bribe_amount = []
    for dec, amt in zip(bribe_df["decimals"], bribe_df["bribe_amount"]):
        decimal = "1"
        decimal = decimal.ljust(dec + 1, "0")
        bribe_amount.append((amt / int(decimal)))

    bribe_df["bribe_amount"] = bribe_amount

    print(bribe_df)
    bribe_df = bribe_df.groupby(by="name")["bribe_amount"].sum().reset_index()
    bribe_df["epoch"] = epoch
    bribe_df["bribe_amount"] = bribe_df["bribe_amount"].astype(float).round(4)
    print(bribe_df)

    # Rewriting current Epoch's Bribe Data
    bribor = pd.read_csv(bribe_csv)
    current_bribe_index = bribor[bribor["epoch"] == epoch].index
    bribor.drop(current_bribe_index, inplace=True)
    bribe_df = pd.concat([bribor, bribe_df], ignore_index=True, axis=0)

    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["bribe_data_sheet_key"]
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    worksheet1.clear()
    set_with_dataframe(
        worksheet=worksheet1,
        dataframe=bribe_df,
        include_index=False,
        include_column_header=True,
        resize=True,
    )

    logger.info("Bribe Data Ended")
except Exception as e:
    logger.error("Error occurred during Bribe Data process. Error: %s" % e)
