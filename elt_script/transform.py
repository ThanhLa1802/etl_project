import logging

def transform_data(df):
    logging.info("Starting data transformation")
    
    # Xóa hàng trống
    df = df.dropna(subset=["First Name", "First Name"])
    
    # Tạo cột full_name
    df["full_name"] = df["First Name"] + " " + df["Last Name"]
    
    # Chuyển tên thành chữ hoa
    df["full_name"] = df["full_name"].str.upper()
    
    logging.info(f"Transformed data: {len(df)} rows")
    return df
