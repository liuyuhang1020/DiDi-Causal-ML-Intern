from abc import abstractmethod
from datetime import datetime, timedelta

import numpy as np
import torch
from torch import nn, optim
from torch.utils import data


class BaseEstimator:
    def __init__(self, df, model, conf):
        """"""
        self.df = df
        self.model = model
        self.conf = conf
        self.optimizer = self.get_optimizer()
        self.preprocess()

    def preprocess(self):
        df, conf = self.df, self.conf
        for row in conf.preprocess_features:
            df[row[0]] = row[1](df, *row[2:])
            print(row[0], df[row[0]].min(), df[row[0]].max())
        df = df.astype({c: np.float32 for c in conf.all_features + conf.labels})
        self.df = df

    @staticmethod
    @abstractmethod
    def loss_fn(pred, y):
        pass

    def train(self, dataloader):
        size = len(dataloader.dataset)
        self.model.train()
        for batch, (X, y) in enumerate(dataloader, 1):
            pred = self.model(X)
            loss = self.loss_fn(pred, y)
            self.optimizer.zero_grad()
            loss.backward()
            self.optimizer.step()
            if self.conf.log_interval and batch % self.conf.log_interval == 0:
                loss, current = loss.item(), batch * len(X)
                print(f'loss: {loss:>7f}  [{current:>5d}/{size:>5d}]')

    def evaluate(self, dataloader):
        batch_num = len(dataloader)
        loss = 0
        predicts, ys = [], []
        self.model.eval()
        with torch.no_grad():
            for X, y in dataloader:
                pred = self.model(X)
                predicts.append(pred)
                ys.append(y)
                loss += self.loss_fn(pred, y)

        loss /= batch_num
        print(f'Avg loss: {loss:>7f}')
        pred = torch.cat(predicts, 0)
        y = torch.cat(ys, 0)
        return pred, y

    def fit(self):
        df, conf = self.df, self.conf
        predicts, ys = [], []
        date_ranges = self.get_date_ranges()
        for i, (begin_dt, end_dt) in enumerate(date_ranges):
            range_df = df.query(f'{conf.dt_col} >= "{begin_dt}" and {conf.dt_col} <= "{end_dt}"').fillna(method='ffill')
            dataset = data.TensorDataset(
                torch.from_numpy(range_df[conf.all_features].values),
                torch.from_numpy(range_df[conf.labels].values),
            )
            dataloader = data.DataLoader(dataset, batch_size=conf.batch_size, shuffle=conf.shuffle)
            if i > 0:
                pred, y = self.evaluate(dataloader)
                predicts.append(pred)
                ys.append(y)
            for _ in range(conf.epoch):
                self.train(dataloader)
            print(list(self.model.parameters()))
        pred = torch.cat(predicts, 0)
        y = torch.cat(ys, 0)
        mae = torch.nn.L1Loss()(pred, y)
        print(f'MAE: {mae}')
        return pred, y

    def predict(self):
        df, conf = self.df, self.conf
        dataset = data.TensorDataset(
            torch.from_numpy(df[conf.all_features].values),
            torch.from_numpy(df[conf.labels].values),
        )
        dataloader = data.DataLoader(dataset, batch_size=conf.batch_size, shuffle=False)
        pred = self.evaluate(dataloader)
        return pred

    def run(self):
        if self.conf.mode == 'predict':
            self.predict()
        else:
            self.fit()

    def get_date_ranges(self):
        conf = self.conf
        date_ranges = [(conf.train_begin, conf.train_end)]
        eval_begin, eval_end = conf.eval_begin, conf.eval_end
        if conf.eval_mode == 'all':
            date_ranges.append((eval_begin, eval_end))
            return date_ranges
        fmt = '%Y-%m-%d'
        begin_dt = datetime.strptime(eval_begin, fmt)
        end_dt = datetime.strptime(eval_end, fmt)
        days = []
        for i in range(1, (end_dt - begin_dt).days):
            day = (begin_dt + timedelta(days=i)).strftime(fmt)
            days.append(day)
        days = [eval_begin] + days + [eval_end]
        if conf.eval_mode == 'day':
            for day in days:
                date_ranges.append((day, day))
        elif conf.eval_mode == 'week':
            for i in range(0, len(days), 7):
                begin = days[i]
                end = days[min(i + 6, len(days) - 1)]
                date_ranges.append((begin, end))
        return date_ranges

    def get_optimizer(self):
        params = self.model.parameters()
        name = self.conf.optimizer.lower()
        lr = self.conf.learning_rate
        if name == 'sgd':
            optimizer = optim.SGD(params, lr=lr)
        elif name == 'adam':
            optimizer = optim.Adam(params, lr=lr)
        else:
            raise ValueError
        return optimizer


class BaseModel(nn.Module):
    def __init__(self, conf):
        super(BaseModel, self).__init__()
        self.conf = conf
        self.feature_idx = {name: i for i, name in enumerate(conf.all_features)}
        self.embedding_dict = self.create_embedding_dict()

    def create_embedding_dict(self):
        conf = self.conf
        embeddings = nn.ModuleDict()
        for row in conf.all_sparse:
            emb_dim, emb_num, emb_name = row[1:]
            if emb_name not in embeddings:
                embeddings[emb_name] = nn.Embedding(emb_num, emb_dim)
        for tensor in embeddings.values():
            nn.init.uniform_(tensor.weight, conf.emb_init_lower, conf.emb_init_upper)
        return embeddings

    def build_inputs(self, x):
        inputs = {
            k: x[:, self.feature_idx[k]:self.feature_idx[k] + 1] for k in self.conf.all_dense
        }
        for feature, _, _, emb_name in self.conf.all_sparse:
            
            inputs[feature] = self.embedding_dict[emb_name](x[:, self.feature_idx[feature]].long())
        return inputs
