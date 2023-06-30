# coding=utf-8
""""数据模块"""
from   data.data_access_object import DAO

if __name__ == '__main__':
    dao = DAO(conf=1)
    '''
    print('testing cr sql')
    a = dao.get_pred_cr()
    print(a.head())
    '''
    print('testing pred_candidate')
    b = dao.get_pred_candidates('testing', 'testing')
    print(b.head())
