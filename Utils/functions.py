import json

def real_time_filter(condition:str, data:str):
    filter_criteria = dict(item.split(":") for item in condition.split(",") if ':' in item)
    data_dict = json.loads(data)
    
    if filter_criteria.get("buildUp", None) is not None:
        buildup = (filter_criteria["buildUp"]).split("+")
        unique_data = list(dict.fromkeys(buildup))
        if data_dict["oi_build_up"] not in unique_data:
            return None
    
        # Add conditions only if the filter_criteria has valid values
    if filter_criteria.get("side", None) is not None:
        if filter_criteria["side"] != data_dict["aggre"]:
            return None

    if filter_criteria.get("sweep", None):
        value = f"""{filter_criteria["sweep"]}Sweep"""
        if data_dict["sweep1"] != value and data_dict["sweep2"] != value and data_dict["sweep3"] != value:
            return None

    if filter_criteria.get("powerSweep", None):
        value = f"""{filter_criteria["powerSweep"]}Sweep"""
        if data_dict["power_sweep"] != value:
            return None

    # if filter_criteria.get("block", None):
    #     if data_dict["block1"] == filter_criteria["block"])

    # if filter_criteria.get("powerBlock", None):
    #     if data_dict["power_block"] == filter_criteria["powerBlock"])

    if filter_criteria.get("optionType", None):
        value = filter_criteria["optionType"]
        if value == "Calls":
            if data_dict["option_type"] != "CE":
                return None
        else:
            if data_dict["option_type"] != "PE":
                return None

    # if filter_criteria.get("optionUnderlier", None):
    #     value = filter_criteria["optionUnderlier"]
    #     if value == "Stock":
    #         if data_dict["option_type"] == "EQ")
    #     else:
    #         if data_dict["option_type"] == "IN")
    
    if filter_criteria.get("sizeFrom", None) is not None:
        if data_dict["last_size"] < filter_criteria["sizeFrom"]:
            return None

    if filter_criteria.get("sizeTo", None) is not None:
        if data_dict["last_size"] > filter_criteria["sizeTo"]:
            return None

    if filter_criteria.get("volumeFrom", None) is not None:
        if data_dict["volume"] < filter_criteria["volumeFrom"]:
            return None

    if filter_criteria.get("volumeTo", None) is not None:
        if data_dict["volume"] > filter_criteria["volumeTo"]:
            return None

    if filter_criteria.get("oiFrom", None) is not None:
        if data_dict["oi"] < filter_criteria["oiFrom"]:
            return None

    if filter_criteria.get("oiTo", None) is not None:
        if data_dict["oi"] > filter_criteria["oiTo"]:
            return None
    
    # OI Change %
    if filter_criteria.get("oiFrom", None) is not None:
        if data_dict["oi"] < filter_criteria["oiFrom"]:
            return None

    if filter_criteria.get("oiTo", None) is not None:
        if data_dict["oi"] > filter_criteria["oiTo"]:
            return None

    if filter_criteria.get("itmFrom", None) is not None:
        if data_dict["strike_difference"] < filter_criteria["itmFrom"]:
            return None

    if filter_criteria.get("itmTo", None) is not None:
        if data_dict["strike_difference"] > filter_criteria["itmTo"]:
            return None
    
    return data