import pandas as pd
import requests
import json
from pdb import set_trace
import time
from datetime import datetime
import os
import yaml
import sys  # モジュール属性 argv を取得するため
from tqdm import tqdm
from itertools import product

f = open("master/columns.yml", "r+")
MASTER_COLUMNS = yaml.load(f, Loader=yaml.FullLoader)


class MasterTable:
    def __init__(self, type):
        self.df = pd.read_csv(f"master/{type}.csv")
        self.type = type

    def get_name(self, code):
        return (
            self.df[self.df[MASTER_COLUMNS[self.type]["code"]] == code][
                MASTER_COLUMNS[self.type]["name"]
            ]
            .to_string(index=False)
            .replace(" ", "")
        )

    @classmethod
    def list(cls):
        master_list = [
            "Area",
            "Pref",
            "GAreaLarge",
            "GAreaMiddle",
            "GAreaSmall",
            "CategoryLarge",
            "CategorySmall",
        ]
        return master_list

    @classmethod
    def get_master_table(cls, name):
        for k1, v1 in MASTER_COLUMNS.items():
            for v2 in v1.values():
                if name == v2:
                    return cls(k1)


class RestaurantTable:
    def __init__(self, df, query):
        self.df = df
        self.query = query

    def to_csv(self, ja=False, excel=False):
        if excel:
            self.delete_line_feed_code()
        file_name = datetime.now().strftime("%Y%m%d")
        for key, value in self.query.items():
            mt = MasterTable.get_master_table(key)
            if ja:
                file_name += "_" + mt.get_name(value)
            else:
                file_name += "_" + value
        self.df.to_csv("results/" + file_name + ".csv")
        return "results/" + file_name + ".csv"

    def delete_line_feed_code(self):
        self.df = self.df.astype(str)
        self.df = self.df.applymap(lambda d: d.replace("\n", " "))


class GrunaviAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.hit_per_page = 100

    def search_all(self, **params):
        pages = self.search(params, count_hits=True) // self.hit_per_page + 1
        if pages == 0:
            return None
        params_with_hit_pages = params.copy()
        params_with_hit_pages["hit_per_page"] = self.hit_per_page
        for offset_page in range(1, pages + 1):
            new_params = params_with_hit_pages.copy()
            new_params["offset_page"] = offset_page
            if "df" in locals():
                df = df.append(self.search(new_params))
            else:
                df = self.search(new_params)
            time.sleep(0.1)
        return RestaurantTable(df, params)

    def search(self, params, count_hits=False):
        query = f"?keyid={self.api_key}"
        for k, v in params.items():
            query += "&" + k + "=" + str(v)
        url = "https://api.gnavi.co.jp/RestSearchAPI/v3/" + query
        r = requests.get(url)
        keys = list(r)
        if count_hits:
            if "total_hit_count" in json.loads(r.text):
                return int(json.loads(r.text)["total_hit_count"])
            else:
                return 0
        try:
            return pd.io.json.json_normalize(json.loads(r.text)["rest"])
        except:
            return pd.DataFrame()

    def master_search(self, name):
        url = f"https://api.gnavi.co.jp/master/{s}SearchAPI/v3/?keyid={ga.api_key}"
        r = requests.get(url)
        dict_r = json.loads(r.text)
        keys = list(dict_r.keys())
        keys.remove("@attributes")
        df = pd.io.json.json_normalize(json.loads(r.text)[keys[0]])
        df.to_csv(f"master/{s}.csv")


if __name__ == "__main__":
    if len(sys.argv) != 2:  # 引数が足りない場合は、その旨を表示
        print("引数にカテゴリ番号を設定してください ex) RSFST01000")
        quit()  # プログラムの終了

    ga = GrunaviAPI(os.getenv("GURUNAVI_API_KEY"))
    master_list = [
        "Area",
        "Pref",
        "GAreaLarge",
        "GAreaMiddle",
        "GAreaSmall",
        "CategoryLarge",
        "CategorySmall",
    ]
    # for s in master_list:
    #     ga.master_search(s)

    df = pd.read_csv("master/GAreaLarge.csv")
    target_area = list(df[df["pref.pref_name"].str.contains('神奈川')].areacode_l)
    if sys.argv[1] == 'all':
        df_category = pd.read_csv("master/CategoryLarge.csv")
        target_category = df_category.category_l_code.tolist()
    else:
        target_category = [sys.argv[1]]
    with tqdm(total=len(target_category) * len(target_area)) as pbar:
        for category_l, areacode_l in product(target_category, target_area):
            rt = ga.search_all(category_l=category_l, areacode_l=areacode_l)
            pbar.update()
            if not rt:
                continue
            res = rt.to_csv(ja=True, excel=True)
