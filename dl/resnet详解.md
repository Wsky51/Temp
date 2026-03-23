# 代码实现如下：
```

# 导入必要的库
import torch
import torch.nn as nn
import torch.nn.functional as F

# ===================== 第一步：定义基础残差块（BasicBlock）=====================
# 适用于 ResNet18/34 的基础残差块（2个3x3卷积）
class BasicBlock(nn.Module):
    # 类属性：卷积层的扩张系数（Bottleneck块会用到，这里先定义为1）
    expansion = 1

    # 初始化函数：定义残差块的核心层
    # in_channels: 输入通道数
    # out_channels: 输出通道数
    # stride: 步幅（默认1，下采样时设为2）
    # downsample: 下采样模块（用于匹配输入输出维度，默认None）
    def __init__(self, in_channels, out_channels, stride=1, downsample=None):
        # 继承父类nn.Module的初始化
        super(BasicBlock, self).__init__()
        
        # 第一个卷积层：3x3卷积，步幅由参数指定，padding=1保证尺寸不变。第一层卷积的核心：特征提取 + 可选下采样 / 通道调整
        self.conv1 = nn.Conv2d(
            in_channels=in_channels,  # 输入通道数
            out_channels=out_channels, # 输出通道数
            kernel_size=3,             # 卷积核大小
            stride=stride,             # 步幅（下采样时用2）
            padding=1,                 # 填充1，保持特征图尺寸不变
            bias=False                 # 后续用BN，bias无意义，节省计算
        )
        # 第一个批归一化层（BN）：加速训练，防止过拟合
        self.bn1 = nn.BatchNorm2d(num_features=out_channels)
        
        # 第二个卷积层：3x3卷积，步幅固定为1（只在第一个卷积下采样）。核心：特征细化 + 维度稳定
        self.conv2 = nn.Conv2d(
            in_channels=out_channels,
            out_channels=out_channels * self.expansion,  # 乘以扩张系数（BasicBlock为1）
            kernel_size=3,
            stride=1,
            padding=1,
            bias=False
        )
        # 第二个批归一化层
        self.bn2 = nn.BatchNorm2d(num_features=out_channels * self.expansion)
        
        # 下采样模块：用于匹配残差连接的维度（通道数/尺寸）
        self.downsample = downsample
        # 步幅参数：保存用于后续判断
        self.stride = stride

    # 前向传播函数：定义数据流向
    def forward(self, x):
        # 保存原始输入（残差连接需要）
        identity = x

        # 第一个卷积 + BN + ReLU
        out = self.conv1(x)    # 卷积
        out = self.bn1(out)    # BN
        out = F.relu(out)      # ReLU激活

        # 第二个卷积 + BN
        out = self.conv2(out)  # 卷积
        out = self.bn2(out)    # BN

        # 残差连接：如果需要下采样，先对原始输入做处理
        if self.downsample is not None:
            identity = self.downsample(x)

        # 核心：残差相加（原始输入 + 卷积输出）
        out += identity
        # 最后ReLU激活
        out = F.relu(out)

        # 返回输出
        return out

# ===================== 2. Bottleneck（用于ResNet50/101/152）=====================
class Bottleneck(nn.Module):
    expansion = 4  # 扩张系数：输出通道数 = 中间3x3卷积通道数 × 4

    def __init__(self, in_channels, out_channels, stride=1, downsample=None):
        super(Bottleneck, self).__init__()
        # 1x1卷积：降维（减少计算量），输入通道=in_channels，输出通道=out_channels
        self.conv1 = nn.Conv2d(
            in_channels=in_channels,
            out_channels=out_channels,
            kernel_size=1,  # 1x1卷积仅调整通道数，不改变尺寸
            stride=1,
            bias=False
        )
        self.bn1 = nn.BatchNorm2d(out_channels)
        
        # 3x3卷积：核心特征提取，输入/输出通道=out_channels
        self.conv2 = nn.Conv2d(
            in_channels=out_channels,
            out_channels=out_channels,
            kernel_size=3,
            stride=stride,  # 仅此处下采样（步幅=2）
            padding=1,
            bias=False
        )
        self.bn2 = nn.BatchNorm2d(out_channels)
        
        # 1x1卷积：升维，输出通道=out_channels × expansion（4倍）
        self.conv3 = nn.Conv2d(
            in_channels=out_channels,
            out_channels=out_channels * self.expansion,
            kernel_size=1,  # 1x1卷积仅调整通道数
            stride=1,
            bias=False
        )
        self.bn3 = nn.BatchNorm2d(out_channels * self.expansion)
        
        self.downsample = downsample  # 维度匹配模块
        self.stride = stride

    def forward(self, x):
        identity = x  # 保存原始输入

        # 1x1卷积 + BN + ReLU（降维）
        out = self.conv1(x)
        out = self.bn1(out)
        out = F.relu(out)

        # 3x3卷积 + BN + ReLU（特征提取）
        out = self.conv2(out)
        out = self.bn2(out)
        out = F.relu(out)

        # 1x1卷积 + BN（升维，暂不激活）
        out = self.conv3(out)
        out = self.bn3(out)

        # 维度匹配：调整原始输入的通道/尺寸
        if self.downsample is not None:
            identity = self.downsample(x)

        # 残差相加
        out += identity
        out = F.relu(out)  # 最终激活

        return out
    
# ===================== 第二步：定义ResNet主网络 =====================
class ResNet(nn.Module):
    # 初始化函数：构建完整的ResNet
    # block: 残差块类型（BasicBlock/Bottleneck）
    # layers: 每个阶段的残差块数量（如ResNet18: [2,2,2,2]）
    # num_classes: 分类任务的类别数（默认1000，ImageNet）
    def __init__(self, block, layers, num_classes=1000):    # 假设训练一张3×224×224的图
        super(ResNet, self).__init__()
        
        # 初始输入通道数（第一个卷积层的输出通道数）
        self.in_channels = 64

        # 第一层：7x7卷积 + BN + ReLU + 最大池化（特征提取的起始层）
        self.conv1 = nn.Conv2d(
            in_channels=3,        # 输入为RGB图像，通道数3
            out_channels=64,      # 输出64通道
            kernel_size=7,        # 7x7大卷积核，捕捉全局特征
            stride=2,             # 步幅2，缩小尺寸
            padding=3,            # padding=3，保持尺寸比例
            bias=False
        )                                           # 输出64×112×112的图
        self.bn1 = nn.BatchNorm2d(num_features=64)  # BN层
        self.relu = nn.ReLU(inplace=True)           # ReLU激活（inplace=True节省内存）
        self.maxpool = nn.MaxPool2d(
            kernel_size=3,        # 3x3池化核
            stride=2,             # 步幅2，进一步缩小尺寸
            padding=1             # padding=1，保持尺寸比例
        )                                           # 输出64×56×56的图

        # 第二层：残差块堆叠（第一个残差阶段，无下采样）
        self.layer1 = self._make_layer(
            block=block,          # 残差块类型
            out_channels=64,      # 输出通道数
            blocks=layers[0],     # 该阶段的块数量
            stride=1              # 步幅1，不缩小尺寸
        )                                           # 输出64×56×56的图
        # 第三层：残差块堆叠（第二个残差阶段，下采样）
        self.layer2 = self._make_layer(
            block=block,
            out_channels=128,
            blocks=layers[1],
            stride=2              # 步幅2，缩小尺寸
        )                                           # 输出128×28×28的图
        # 第四层：残差块堆叠（第三个残差阶段，下采样）
        self.layer3 = self._make_layer(
            block=block,
            out_channels=256,
            blocks=layers[2],
            stride=2
        )                                           # 输出256×14×14的图
        # 第五层：残差块堆叠（第四个残差阶段，下采样）
        self.layer4 = self._make_layer(
            block=block,
            out_channels=512,
            blocks=layers[3],
            stride=2
    )                                              # 输出512×7×7的图

        # 全局平均池化：将特征图转为固定维度的向量（7x7->1x1）
        self.avgpool = nn.AdaptiveAvgPool2d((1, 1)) # 512×1×1
        # 全连接层：将特征向量映射到分类结果
        self.fc = nn.Linear(512 * block.expansion, num_classes)

        # 权重初始化：对卷积层和BN层做默认初始化
        for m in self.modules():
            if isinstance(m, nn.Conv2d):
                nn.init.kaiming_normal_(m.weight, mode='fan_out', nonlinearity='relu')
            elif isinstance(m, nn.BatchNorm2d):
                nn.init.constant_(m.weight, 1)
                nn.init.constant_(m.bias, 0)

    # 辅助函数：构建残差块的堆叠层（核心复用逻辑）
    # block: 残差块类型
    # out_channels: 每个块的输出通道数
    # blocks: 该层需要堆叠的块数量
    # stride: 第一个块的步幅（下采样用）
    def _make_layer(self, block, out_channels, blocks, stride=1):
        # 初始化下采样模块为None
        downsample = None
        # 判断是否需要下采样：
        # 1. 步幅!=1（尺寸变化） 2. 输入通道数!=输出通道数*扩张系数（通道数变化）
        if stride != 1 or self.in_channels != out_channels * block.expansion:
            downsample = nn.Sequential(
                # 1x1卷积：调整通道数和尺寸（下采样核心）
                nn.Conv2d(
                    self.in_channels,
                    out_channels * block.expansion,
                    kernel_size=1,  # 1x1卷积不改变空间维度，只调整通道
                    stride=stride,  # 步幅匹配，缩小尺寸
                    bias=False
                ),
                # BN层：配合卷积层
                nn.BatchNorm2d(out_channels * block.expansion)
            )

        # 初始化残差块列表
        layers = []
        # 添加第一个残差块（可能包含下采样）
        layers.append(
            block(
                in_channels=self.in_channels,
                out_channels=out_channels,
                stride=stride,
                downsample=downsample
            )
        )
        # 更新输入通道数（后续块的输入通道数 = 当前输出通道数*扩张系数）
        self.in_channels = out_channels * block.expansion

        # 添加剩余的残差块（步幅固定为1，无下采样）
        for _ in range(1, blocks):
            layers.append(
                block(
                    in_channels=self.in_channels,
                    out_channels=out_channels
                )
            )

        # 将列表转为Sequential（PyTorch的有序容器）
        return nn.Sequential(*layers)

    # 前向传播：定义完整的数据流向
    def forward(self, x):
        # 第一层：7x7卷积 + BN + ReLU + 最大池化
        x = self.conv1(x)
        x = self.bn1(x)
        x = self.relu(x)
        x = self.maxpool(x)

        # 残差块堆叠层
        x = self.layer1(x)
        x = self.layer2(x)
        x = self.layer3(x)
        x = self.layer4(x)

        # 全局平均池化 + 全连接分类
        x = self.avgpool(x)       # [batch, 512, 1, 1]
        x = torch.flatten(x, 1)   # 展平：[batch, 512]
        x = self.fc(x)            # 分类：[batch, num_classes]

        return x

# ===================== 第三步：封装常用的ResNet模型 =====================
# ResNet18
def resnet18(num_classes=1000):
    return ResNet(BasicBlock, [2, 2, 2, 2], num_classes=num_classes)

# ResNet34
def resnet34(num_classes=1000):
    return ResNet(BasicBlock, [3, 4, 6, 3], num_classes=num_classes)

def resnet50(num_classes=1000):
    # Bottleneck + [3,4,6,3] → ResNet50
    return ResNet(Bottleneck, [3, 4, 6, 3], num_classes)

def resnet101(num_classes=1000):
    # Bottleneck + [3,4,23,3] → ResNet101
    return ResNet(Bottleneck, [3, 4, 23, 3], num_classes)

# ===================== 测试代码 =====================
if __name__ == "__main__":
    # 创建ResNet18模型
    model = resnet18(num_classes=10)  # 改为10分类（如CIFAR-10）
    # 打印模型结构（可选）
    # print(model)
    
    # 测试前向传播：生成一个随机输入（batch_size=2, 3通道, 224x224）
    input_tensor = torch.randn(2, 3, 224, 224)
    print(f"input_tensor:{input_tensor}")
    # 前向传播
    output = model(input_tensor)
    # 打印输出形状（应为 [2, 10]）
    print(f"输出形状: {output.shape}")

```
