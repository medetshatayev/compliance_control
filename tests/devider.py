import pandas as pd
import os

def split_csv_with_headers(input_file, num_parts=4):
    """
    Разделяет CSV файл на части с сохранением заголовков
    """
    print(f"Читаем файл {input_file}...")
    
    # Читаем CSV
    df = pd.read_csv(input_file)
    
    total_rows = len(df)
    chunk_size = total_rows // num_parts
    
    print(f"Всего строк: {total_rows}")
    print(f"Разделяем на {num_parts} частей")
    print(f"Примерно {chunk_size} строк на файл\n")
    
    total_size = 0
    
    for i in range(num_parts):
        # Определяем границы
        start = i * chunk_size
        # Последний файл получает все оставшиеся строки
        end = start + chunk_size if i < num_parts - 1 else total_rows
        
        # Вырезаем chunk
        chunk = df.iloc[start:end]
        
        # Сохраняем с заголовком (pandas автоматически добавляет header)
        output_file = f'OFAC_part_{i+1}.csv'
        chunk.to_csv(output_file, index=False)
        
        # Проверяем размер
        file_size = os.path.getsize(output_file) / (1024 * 1024)  # в МБ
        total_size += file_size
        
        print(f"✓ {output_file}: {len(chunk)} строк, {file_size:.2f} МБ")
    
    print(f"\nОбщий размер всех частей: {total_size:.2f} МБ")
    print(f"Исходный файл: {os.path.getsize(input_file) / (1024 * 1024):.2f} МБ")
    
    # Проверка целостности
    print("\n--- Проверка ---")
    print(f"Строк в исходном файле: {total_rows}")
    
    total_rows_in_parts = 0
    for i in range(num_parts):
        part_df = pd.read_csv(f'OFAC_part_{i+1}.csv')
        total_rows_in_parts += len(part_df)
    
    print(f"Строк во всех частях: {total_rows_in_parts}")
    
    if total_rows == total_rows_in_parts:
        print("✓ Все строки сохранены корректно!")
    else:
        print("✗ ОШИБКА: Количество строк не совпадает!")

# Запуск
if __name__ == "__main__":
    split_csv_with_headers('TERRORISTS_OFAC.csv', num_parts=4)