import torch
from torch import nn

from base_estimator import BaseEstimator, BaseModel


class LogPriorNN(BaseModel):
    def __init__(self, conf):
        super(LogPriorNN, self).__init__(conf)
        self.nn_stack = nn.Sequential(
            nn.BatchNorm1d(25),
            nn.Linear(25, 20),
            nn.ReLU(),
            nn.Linear(20, 1),
        )
        # initialization
        for layer in self.nn_stack:
            for name, tensor in layer.named_parameters():
                if name == 'weight':
                    nn.init.normal_(tensor, mean=0, std=conf.init_std)
        self.scale = nn.Parameter(torch.ones(1, 1, dtype=torch.float32))

    def forward(self, x):
        conf = self.conf
        inputs = self.build_inputs(x)
        feature_names = conf.all_dense + [row[0] for row in conf.all_sparse]
        feature_input = torch.cat([inputs[k] for k in feature_names], 1)
        treatment_input = torch.cat([inputs[k] for k in conf.treatments], 1)
        out = self.nn_stack(feature_input)
        out = out * torch.log(self.scale * treatment_input + 1)
        return out


class Estimator(BaseEstimator):
    @staticmethod
    def loss_fn(pred, y):
        mse = nn.MSELoss()
        return mse(pred, y)
