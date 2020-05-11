"""
Simple wrapper to access Microsoft Business Central data through its Web Services
author: Francesco Baldisserri
email: fbaldisserri@gmail.com
version: 0.8
"""

import numpy as np
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth


ODATA_HEADER = {
    "OData-Version": "4.0",
    "Accept": "application/json",
    "Content-Type": "application/json; charset=utf-8",
    "Prefer": "odata.maxpagesize=100",
    "Prefer": "odata.include-annotations=OData.Community.Display.V1.FormattedValue"
}

ODATA_ROOT = "https://api.businesscentral.dynamics.com/v1.0/"


class BcWrapper:

    def __init__(self, tenant, company, user, password, verbose=False):
        """
        Initialization that connect to Business Central services

        :param tenant: BC Server Tenant (e.g. 'cbef3105-140c-4614-b685-61d44b282736')
        :param company: BC Company Name (e.g. 'CRONUS US')
        :param user: BC Web services user
        :param password: BC Web services password
        :param verbose: Verbose mode
        """
        self.url = f"{ODATA_ROOT}{tenant}/ODataV4/Company('{company}')/"
        self.auth = HTTPBasicAuth(user, password)
        self.verbose = verbose

    def get_table(self, table, options={}, table_index=None):
        """
        Download table data from Business Central

        :param table: BC Data table to get
        :param options: OData options dictionary as per OData specs (optional)
        :param table_index: Optional list of strings for setting the returned dataframe index (optional)
        :return: Dataframe with BC product information (Article No. as index and field name as column names)
        """
        query = self.url + table  # full path to web api endpoint

        response = requests.get(query,
                                auth=self.auth,
                                headers=ODATA_HEADER,
                                params=options).json()

        if is_valid(response):
            data = pd.DataFrame.from_records(get_data(response))
            while has_more_pages(response):
                response = requests.get(get_next_page(response), auth=self.auth,
                                        headers=ODATA_HEADER).json()
                data = data.append(pd.DataFrame.from_records(get_data(response)))
            if self.verbose:
                print(f"Downloaded {len(data)} records from {query}")

            data[(data.isnull()) | (data == '')] = np.nan

            if table_index is not None:
                data.set_index(table_index, inplace=True)

            if '$fields' in options:
                data = data[options['$fields']] if len(data)>0 \
                    else pd.DataFrame(columns=options['$fields'])
            return data
        else:
            raise Exception("%s\t%s" % (response["error"]["code"],
                                        response["error"]["message"]))

    def get_ats(self, warehouses=None, po_table='purchaseDocumentLines',
                oh_table='ItemLedgerEntries', so_table='salesDocumentLines',
                wship_table='WarehouseShipmentsLines'):
        """
        Gets ATS availability from Business Central

        :param warehouses: list of warehouses to be included
        (optional, all warehouses included if nothing is specified)
        :param po_table: purchase orders table name to be used (optional)
        :param oh_table: on hand table name to be used (optional)
        :param so_table: sales orders table name to be used (optional)
        :param wship_table: warehouse shipment table name to be used (optional)
        :return: Dataframe with PO, OH, SO, ATShip and ATSell by warehouse and SKU
        """

        # Business Central data download and preparation
        purchases = self.get_purchases(po_table)
        onhand = self.get_onhand(oh_table)
        sales = self.get_sales(so_table)
        w_shipments = self.get_warehouse_shipments(wship_table)
        # TODO: Reintroduce transfers?

        # Joining purchase, inventory, sale and shipments data
        ats = onhand.append([purchases, sales, w_shipments]).fillna(0)
        if warehouses is not None:  # TODO: Remove when ODATA operator '$filter IN' is working
            ats = ats[ats['warehouse'].isin(warehouses)]
        ats = ats.groupby(['warehouse', 'sku'], sort=False).sum()
        ats['OH'] -= ats['WSHIP']  # Removing Items about to be shipped from OH
        ats['SO'] -= ats['WSHIP']  # Removing Items about to be shipped from SO
        ats['ATSell'] = ats['PO'] + ats['OH'] - ats['SO']
        ats['ATShip'] = np.minimum(ats['ATSell'], ats['OH'])

        # Formatting ATS data
        ats = ats.reset_index().pivot_table(
            index='sku', columns='warehouse', aggfunc='sum', fill_value=0
        )
        columns_to_keep = [(b, a) for a in warehouses
                           for b in ['PO', 'OH', 'SO', 'ATShip', 'ATSell']]
        return ats[columns_to_keep]

    def get_purchases(self, po_table):
        options = {'$fields': ['number', 'locationCode', 'outstandingQuantity'],
                   '$filters': ['outstandingQuantity ne 0']}
        return self.get_table(table=po_table, options=options)\
            .set_axis(['sku', 'warehouse', 'PO'], axis='columns', inplace=False)

    def get_onhand(self, oh_table):
        options = {'$fields': ['Item_No', 'Location_Code', 'Quantity'],
                   '$filters': ['Quantity ne 0']}
        return self.get_table(table=oh_table, options=options)\
            .set_axis(['sku', 'warehouse', 'OH'], axis='columns', inplace=False)

    def get_sales(self, so_table):
        options = {
            '$fields': ['number', 'locationCode', 'outstandingQuantity'],
            '$filters': ['outstandingQuantity ne 0']
        }
        return self.get_table(table=so_table, options=options)\
            .set_axis(['sku', 'warehouse', 'SO'], axis='columns', inplace=False)

    def get_warehouse_shipments(self, wship_table):
        options = {
            '$fields': ['Item_No', 'Location_Code', 'Qty_Outstanding'],
            '$filters': ['Qty_Outstanding ne 0']
        }
        return self.get_table(table=wship_table, options=options)\
            .set_axis(['sku', 'warehouse', 'WSHIP'], axis='columns', inplace=False)


def has_more_pages(response):
    return "@odata.nextLink" in response


def get_next_page(response):
    return response["@odata.nextLink"]


def is_valid(response):
    return 'value' in response


def get_data(response):
    return response["value"]
