from business.etl import extract, transform, load
from business.model import create_model

if __name__ == '__main__':
    raw_df = extract()
    modified_df = transform(raw_df)
    print(raw_df.head(5))
    print(modified_df.head(5))
    print(modified_df.isna().sum())
    model = create_model(modified_df.iloc[0])
    print(model.attendance_rate)
    models = modified_df.apply(create_model, axis=1)
    print(models.values[10])
    modified_df = modified_df.tail(10)
    load(modified_df)