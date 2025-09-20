# mask.py
import re
import pandas as pd

def mask_email(email):
    if pd.isna(email):
        return email
    parts = email.split('@')
    return parts[0][0] + "*****@" + parts[1] if len(parts) > 1 else email

def mask_phone(phone):
    if pd.isna(phone):
        return phone
    phone_str = str(phone)
    return re.sub(r'\d(?=\d{4})', '*', phone_str)

def mask_aadhaar(aadhaar):
    if pd.isna(aadhaar):
        return aadhaar
    aadhaar_str = str(aadhaar)
    return re.sub(r'\d(?=\d{4})', '*', aadhaar_str)

def mask_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    masked_df = df.copy()
    if 'email' in masked_df.columns:
        masked_df['email'] = masked_df['email'].apply(mask_email)
    if 'phone' in masked_df.columns:
        masked_df['phone'] = masked_df['phone'].apply(mask_phone)
    if 'aadhaar' in masked_df.columns:
        masked_df['aadhaar'] = masked_df['aadhaar'].apply(mask_aadhaar)
    return masked_df
