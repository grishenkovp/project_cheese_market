import pandas as pd
import yaml
import scipy.stats as stats
from scipy.stats import mannwhitneyu, f_oneway
from typing import List
import model as m

with open('settings.yaml', encoding='utf8') as f:
    settings = yaml.safe_load(f)

path_data_sales_group = settings['DATA']['PATH_SALES_GROUP']
file_name_sales_group = settings['DATA']['FILE_NAME_SALES_GROUP']

path_file = '{path_file}{file_name}'.format(path_file=path_data_sales_group, file_name=file_name_sales_group)
df = pd.read_csv(path_file, sep=';', decimal=',')

total_proceeds1 = list(df[(df['model'] == m.AB_TEST_NUMBER_FIRST_MODEL) \
                          & (df['retailer'] == m.AB_TEST_NAME_RETAILER)]['total_proceeds'])

total_proceeds2 = list(df[(df['model'] == m.AB_TEST_NUMBER_SECOND_MODEL) \
                          & (df['retailer'] == m.AB_TEST_NAME_RETAILER)]['total_proceeds'])

total_proceeds3 = list(df[(df['model'] == m.AB_TEST_NUMBER_THIRD_MODEL) \
                          & (df['retailer'] == m.AB_TEST_NAME_RETAILER)]['total_proceeds'])


def func_shapiro(x: List) -> bool:
    """Тест Шапиро-Уилка"""
    stat, p = stats.shapiro(x)
    alpha = 0.05
    if p > alpha:
        return True
    else:
        return False


def func_ttest(x: List, y: list) -> str:
    """Т-критерий Стьюдента"""
    stat, p = stats.ttest_ind(x, y, alternative="two-sided")
    alpha = 0.05
    if p > alpha:
        print('Различий в эффекте нет')
    else:
        print('Различия в эффекте есть')


def func_mannwhitneyu(x: List, y: list) -> str:
    """Критерий Манна — Уитни"""
    stat, p = mannwhitneyu(x, y)
    alpha = 0.05
    if p > alpha:
        print('Различий в эффекте нет')
    else:
        print('Различия в эффекте есть')


# def func_f_oneway(x: list, y: list, z: list) -> str:
#     """ANOVA"""
#     f, p = f_oneway(x, y, z)
#     alpha = 0.05
#     if p > alpha:
#         print('Различий в эффекте нет')
#     else:
#         print('Различия в эффекте есть')

if func_shapiro(total_proceeds1) & func_shapiro(total_proceeds2):
    print('Т-критерий Стьюдента')
    func_ttest(total_proceeds1, total_proceeds2)
else:
    print('Критерий Манна — Уитни')
    func_mannwhitneyu(total_proceeds1, total_proceeds2)

# print('ANOVA')
# func_f_oneway(total_proceeds1, total_proceeds2,total_proceeds3)
