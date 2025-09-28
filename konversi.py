# excel_to_json_converter.py
#
# Deskripsi:
# Script ini membaca data pesanan dari file Excel (.xlsx) yang berisi sheet 'order_data' dan 'order_detail',
# menggabungkannya, dan mengonversinya menjadi satu file JSON dengan struktur bersarang.
#
# Kebutuhan Library:
# Pastikan Anda sudah menginstal library pandas, openpyxl, dan numpy. Jika belum, jalankan perintah ini di terminal:
# pip install pandas openpyxl numpy

import pandas as pd
import json
import numpy as np
import os
import shutil
from datetime import datetime

def default_serializer(obj):
    """
    JSON serializer untuk menangani objek yang tidak bisa diserialisasi secara default,
    khususnya tipe data integer dari NumPy.
    """
    if isinstance(obj, np.integer):
        return int(obj)
    raise TypeError

def convert_excel_to_json(excel_file_path, output_json_path):
    """
    Mengonversi file Excel dengan dua sheet menjadi satu file JSON terstruktur.

    - Jika ada banyak order di sheet `order_data`, hasil JSON berisi list dari semua order.
    - Data `order_detail` dihubungkan ke setiap order melalui kolom `order_data_id` → `order_data.id`.

    Args:
        excel_file_path (str): Path ke file .xlsx input.
        output_json_path (str): Path untuk menyimpan file JSON hasil konversi.
    """
    try:
        # 1. Baca data dari sheet 'order_data' dan 'order_detail' di dalam file Excel
        # Pandas akan menggunakan engine 'openpyxl' untuk membaca file .xlsx
        print(f"Membaca file Excel dari: {excel_file_path}")
        order_df = pd.read_excel(excel_file_path, sheet_name='order_data')
        # Pastikan 'pack_id' dibaca sebagai string agar leading zero tidak hilang
        detail_df = pd.read_excel(
            excel_file_path,
            sheet_name='order_detail',
            dtype={"pack_id": str, "product_id": str}
        )
        # Normalisasi nilai pack_id: None jika kosong, dan jaga leading zero minimal 4 digit jika numerik
        def _normalize_pack_id(v):
            if v is None:
                return None
            s = str(v).strip()
            if s == '' or s.lower() == 'nan':
                return None
            # Jika murni digit tanpa leading zero dari Excel, coba padding ke 4 digit
            if s.isdigit() and len(s) < 4:
                return s.zfill(4)
            return s
        if 'pack_id' in detail_df.columns:
            detail_df['pack_id'] = detail_df['pack_id'].map(_normalize_pack_id)
        # Normalisasi product_id/SKU menjadi string apa adanya (trim), tanpa konversi numerik
        def _normalize_product_id(v):
            if v is None:
                return None
            s = str(v).strip()
            if s == '' or s.lower() == 'nan':
                return None
            # Hilangkan akhiran .0 jika datang dari excel sebagai 3834...0.0 yang dikonversi ke string
            if s.endswith('.0') and s.replace('.', '', 1).isdigit():
                s = s[:-2]
            return s
        if 'product_id' in detail_df.columns:
            detail_df['product_id'] = detail_df['product_id'].map(_normalize_product_id)
        # Normalisasi UOM menjadi string (baik uom item maupun conversion_uom)
        def _normalize_text(v):
            if v is None:
                return None
            s = str(v).strip()
            if s == '' or s.lower() == 'nan':
                return None
            return s
        if 'uom' in detail_df.columns:
            detail_df['uom'] = detail_df['uom'].map(_normalize_text)
        if 'conversion_uom' in detail_df.columns:
            detail_df['conversion_uom'] = detail_df['conversion_uom'].map(_normalize_text)
        print("Berhasil membaca kedua sheet.")

        # Validasi minimal
        if order_df.empty:
            print("Error: Sheet 'order_data' di dalam file Excel kosong.")
            return

        # Kolom yang mendefinisikan sebuah item unik (di dalam satu order)
        item_identifier_cols = [
            'line_id', 'product_id', 'product_description', 'group_id',
            'group_description', 'product_type', 'qty', 'uom', 'pack_id',
            'product_net_price'
        ]

        all_orders = []

        # 2. Iterasi setiap order pada sheet 'order_data'
        for _, order_row in order_df.iterrows():
            # Konversi satu baris order menjadi dict, ganti NaN → None
            order_header = order_row.where(pd.notna(order_row), None).to_dict()

            # 3. Ambil detail khusus untuk order ini menggunakan relasi order_data_id → id
            if 'id' not in order_row or 'order_data_id' not in detail_df.columns:
                raise ValueError("Kolom relasi 'id' pada order_data atau 'order_data_id' pada order_detail tidak ditemukan.")

            order_details = detail_df[detail_df['order_data_id'] == order_row['id']]

            # Jika tidak ada detail, tetap hasilkan order dengan items kosong
            if order_details.empty:
                order_header['items'] = []
                all_orders.append(order_header)
                continue

            # 4. Kelompokkan detail berdasarkan item unik (hanya dalam ruang lingkup order ini)
            grouped_items = order_details.groupby(item_identifier_cols, dropna=False)

            items_list = []
            for item_keys, group in grouped_items:
                item_dict = dict(zip(item_identifier_cols, item_keys))
                # Pastikan SKU sebagai string
                if 'product_id' in item_dict and item_dict['product_id'] is not None:
                    item_dict['product_id'] = str(item_dict['product_id'])
                # Pastikan UOM sebagai string
                if 'uom' in item_dict and item_dict['uom'] is not None:
                    item_dict['uom'] = str(item_dict['uom'])

                conversions = []
                for _, row in group.iterrows():
                    conv_uom_val = row['conversion_uom'] if 'conversion_uom' in row else None
                    conversions.append({
                        "uom": (str(conv_uom_val) if pd.notna(conv_uom_val) and conv_uom_val is not None else None),
                        "numerator": int(row['conversion_numerator']),
                        "denominator": int(row['conversion_denominator'])
                    })

                item_dict['conversion'] = conversions
                items_list.append(item_dict)

            # 5. Gabungkan header + items untuk order ini
            order_header['items'] = items_list
            all_orders.append(order_header)

        # 6. Tulis hasil akhir ke dalam file JSON
        #    - Jika hanya ada satu order, tulis sebagai object
        #    - Jika lebih dari satu, tulis sebagai list of objects
        payload = all_orders[0] if len(all_orders) == 1 else all_orders

        json_output_string = json.dumps(payload, indent=2, default=default_serializer)

        with open(output_json_path, 'w') as json_file:
            json_file.write(json_output_string)

        print("\n--- HASIL KONVERSI JSON ---")
        print(json_output_string)
        print("---------------------------\n")

        print(f"Konversi berhasil! File JSON juga telah disimpan di: {output_json_path}")

        # 7. Setelah sukses, rename file Excel input menjadi `<basename>_YYYYMMDD_HHMMSS.xlsx`
        try:
            excel_dir = os.path.dirname(excel_file_path) or "."
            base_name = os.path.splitext(os.path.basename(excel_file_path))[0]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_excel_name = f"{base_name}_{timestamp}.xlsx"
            new_excel_path = os.path.join(excel_dir, new_excel_name)
            os.rename(excel_file_path, new_excel_path)
            print(f"File Excel telah di-rename menjadi: {new_excel_name}")

            # 8. Pindahkan file yang sudah di-rename ke folder arsip
            archive_dir = os.path.join(excel_dir, "data_archive_order")
            os.makedirs(archive_dir, exist_ok=True)
            destination_path = os.path.join(archive_dir, new_excel_name)
            shutil.move(new_excel_path, destination_path)
            print(f"File Excel dipindahkan ke folder arsip: {destination_path}")
        except Exception as e:
            print(f"Gagal me-rename / memindahkan file Excel: {e}")

    except FileNotFoundError:
        print(f"Error: File tidak ditemukan. Pastikan file '{excel_file_path}' ada di direktori yang benar.")
    except ValueError as e:
        print(f"Error: Sheet/kolom tidak cocok. Pastikan ada sheet 'order_data' dan 'order_detail' serta kolom relasi yang benar. Detail: {e}")
    except Exception as e:
        print(f"Terjadi error: {e}")


# --- CARA MENGGUNAKAN SCRIPT ---
if __name__ == "__main__":
    # Tentukan nama file input dan output.
    # Pastikan file Excel berada di folder yang sama dengan script ini,
    # atau ubah path-nya sesuai lokasi file Anda.
    input_excel_file = "template_feed_order.xlsx"
    output_json_file = "output_order.json"
    
    # Panggil fungsi konversi
    convert_excel_to_json(input_excel_file, output_json_file)

