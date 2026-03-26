# Transformer 底层技术原理全拆解
Transformer是2017年Google在《Attention Is All You Need》中提出的序列建模架构，完全基于注意力机制实现，彻底抛弃了RNN系列的串行结构，解决了长序列建模的**梯度消失、并行计算效率低**两大核心痛点，是当前所有大语言模型、多模态模型的基础架构。

本文从**核心动机→基础单元→完整架构→训练推理→核心优势**，逐层拆解Transformer的底层原理。

---

## 一、Transformer解决的核心痛点
在Transformer之前，序列建模的主流方案是RNN/LSTM/GRU，存在两个致命缺陷：
1. **串行计算，无法并行**：每个时刻的输出必须依赖上一个时刻的结果，无法利用GPU的并行计算能力，训练效率极低，长序列场景尤为明显。
2. **长距离依赖捕捉能力弱**：序列信息需要一步步传递，长序列中前面的信息会被逐步稀释，同时伴随梯度消失问题，很难学到跨很远位置的依赖关系（比如长文本的指代关系）。

CNN虽然可以并行，但感受野有限，要捕捉长距离依赖需要堆叠大量卷积层，效率和效果都不理想。

Transformer的核心突破：**用自注意力机制，一步实现序列中任意两个token的信息交互，同时支持全序列并行计算**，完美解决了上述两个问题。

---

## 二、核心基础：缩放点积注意力（Scaled Dot-Product Attention）
自注意力是Transformer的最小核心单元，它的本质是：**给序列中的每个token，计算它和全序列所有token（包括自身）的关联权重，用权重对所有token的特征加权求和，得到融合了全序列上下文的新特征**。

### 2.1 核心概念：Q、K、V向量
每个输入token的嵌入向量（Embedding），会通过3个可学习的权重矩阵，生成3个不同的向量：
- **Query（ $Q$，查询向量）**：当前token的「搜索关键词」，用来匹配其他token的相关性。
- **Key（ $K$，键向量）**：其他token的「匹配标签」，用来和 $Q$计算相似度。
- **Value（ $V$，值向量）**：token的「核心内容」，最终用来加权求和的特征。

> 类比理解：你在搜索引擎里搜「Transformer原理」（ $Q$），引擎会匹配所有网页的标题（ $K$），得到每个网页的匹配度，再按匹配度加权，把网页的内容（ $V$）整合起来给你，就是自注意力的计算逻辑。

### 2.2 完整计算步骤（公式+通俗解释）
标准的缩放点积注意力公式：

$$Attention(Q,K,V) = Softmax\left( \frac{QK^T}{\sqrt{d_k}} \right) V $$

分4步拆解：
1. **计算相似度分数**：用 $Q$和 $K$的点积，计算当前token和每个token的匹配度，点积结果越大，相关性越高。
2. **缩放（Scaled）**：把点积结果除以 $\sqrt{d_k}$（ $d_k$是 $K$向量的维度）。
   - 底层原因：若 $Q$、 $K$的每个元素都是均值0、方差1的随机变量，点积结果的方差为 $d_k$，维度越大，点积值的波动越大，会导致Softmax后输出接近one-hot分布，梯度几乎为0，无法训练。缩放后把方差拉回1，稳定梯度。
3. **掩码（Mask，可选）**：把不需要关注的位置的分数设为负无穷，Softmax后权重会变成0。
   - 常见掩码：因果掩码（Decoder用，遮挡未来的token，防止生成时提前看到后面的内容）、Padding掩码（遮挡输入的填充位，不让模型关注无效的Padding）。
4. **Softmax+加权求和**：Softmax把分数转为0-1之间、和为1的权重，再乘以 $V$，加权求和得到当前token的最终注意力输出。

---

## 三、进阶核心：多头注意力（Multi-Head Attention, MHA）
单头注意力只能捕捉一种类型的依赖关系，而多头注意力是对单头注意力的扩展，也是Transformer的核心设计之一。

### 3.1 核心逻辑
把 $Q$、 $K$、 $V$通过不同的线性投影，映射到 $h$个不同的特征子空间，每个子空间独立做一次单头注意力计算，得到 $h$个独立的注意力输出，再把所有头的结果拼接起来，经过一次线性投影，得到最终的多头注意力输出。

公式：

$$MultiHead(Q,K,V) = Concat(head_1, head_2, ..., head_h) W_o$$

$$其中\ head_i = Attention(Q W_{q_i}, K W_{k_i}, V W_{v_i})$$

### 3.2 设计意义
- **多语义捕捉**：不同的头可以学习不同类型的依赖关系，比如有的头关注相邻的语法关系，有的头关注长距离的指代关系，有的头关注核心语义的匹配，大幅提升模型的特征表达能力。
- **计算量可控**：原文中 $d_{model}=512$，头数 $h=8$，每个头的维度 $d_k=d_v=512/8=64$，总计算量和单头注意力基本一致，没有额外的开销。

---

## 四、Transformer完整架构
Transformer是标准的Encoder-Decoder（编码器-解码器）架构，左侧为编码器，右侧为解码器，原文中编码器和解码器各堆叠6个完全相同的层。


<div align="center">
  <img src="images/transformer_summary.png" alt="编码示例" title="编码示例" width="800" />
</div>

<div align="center">
  <img src="images/transformer_arch.png" alt="架构图" title="架构图" width="600" />
</div>


### 4.1 前置处理：位置编码（Positional Encoding）
Transformer没有RNN的串行结构，天然没有序列的位置信息，没有位置编码的话，模型会把输入当成词袋，序列顺序变化输出不变，因此必须手动给每个token注入位置信息。

原文使用正弦余弦位置编码，公式：

$$PE_{(pos, 2i)} = sin\left( \frac{pos}{10000^{2i/d_{model}}} \right)$$

$$PE_{(pos, 2i+1)} = cos\left( \frac{pos}{10000^{2i/d_{model}}} \right)$$

-  $pos$：token在序列中的位置， $i$是特征维度的索引。
- 设计优势：每个位置的编码唯一；可以学习到token之间的相对位置关系；可以泛化到训练时没见过的更长序列。

最终输入编码器的特征 = token的嵌入向量 + 对应位置的位置编码。

---

### 4.2 编码器（Encoder）
编码器的作用是对输入序列进行编码，提取全序列的上下文语义特征，输出给解码器使用。

每个编码器层包含2个核心子层，所有子层都带**残差连接+层归一化**，结构为： $LayerNorm(x + Sublayer(x))$
1. **多头自注意力层**： $Q$、 $K$、 $V$都来自上一层的输出，是序列自身和自身的注意力，捕捉输入序列内部的双向依赖关系（每个token都能看到全序列的所有token）。
2. **前馈神经网络（FFN）**：对每个token的特征独立做非线性变换，公式为：

$$FFN(x) = max(0, xW_1 + b_1)W_2 + b_2$$

> 注意力是线性的加权求和，FFN提供非线性变换，大幅提升模型的表达能力。

#### 残差连接+层归一化的作用
- 残差连接：解决深层网络的梯度消失问题，让几十上百层的Transformer可以稳定训练。
- 层归一化（LayerNorm）：对每个样本的特征做归一化（均值0、方差1），不依赖batch大小，适配NLP的变长序列，稳定训练过程，加速收敛。

---

### 4.3 解码器（Decoder）
解码器的作用是基于编码器的输出，自回归地生成目标序列。

每个解码器层包含3个核心子层，同样都带残差连接+层归一化：
1. **掩码多头自注意力层**：加入因果掩码，只允许当前token看到它之前的token，遮挡未来的token，符合文本生成从左到右的逻辑，防止信息泄露。
2. **交叉注意力（Encoder-Decoder Attention）**： $Q$来自解码器上一层的输出， $K$和 $V$来自编码器的最终输出，让解码器在生成每个token时，能关注到输入序列的相关部分（比如翻译时，生成英文单词对应到中文原词）。
3. **前馈神经网络（FFN）**：和编码器中的FFN完全一致。

---

### 4.4 输出层
解码器的最终输出，经过线性层把特征维度映射为词表大小，再经过Softmax得到每个token的概率分布，取概率最大的token作为当前步的输出，再把这个输出作为下一步的输入，继续生成，直到遇到结束符。

---

## 五、Transformer的训练与推理逻辑
### 5.1 训练阶段（全并行）
训练时可以输入完整的目标序列，通过掩码遮挡未来的token，一次性计算所有位置的预测结果和交叉熵损失，完全并行计算，没有串行依赖，训练效率远高于RNN系列。

### 5.2 推理阶段（自回归串行）
推理时是串行的，生成第 $t$个token时，只能使用前 $t-1$个已经生成的token作为输入，一步步自回归生成，直到输出结束符。

---

## 六、Transformer的主流变体
原生的Encoder-Decoder架构多用于机器翻译、摘要等条件生成任务，当前主流大模型都基于Transformer的简化变体：
1. **Encoder-only架构（双向Transformer）**：代表是BERT，只有编码器，使用双向自注意力，适合文本理解类任务（分类、NER、阅读理解、语义匹配）。
2. **Decoder-only架构（单向Transformer）**：代表是GPT系列、LLaMA系列，只有解码器，使用带掩码的单向自注意力，适合文本生成、对话等自回归生成任务，也是当前通用大语言模型的主流架构。
3. **Encoder-Decoder架构**：代表是T5、BART，保留完整的编解码结构，适合机器翻译、文本摘要、图文生成等条件生成任务。

---

## 七、Transformer的核心优势
1. **极致的长距离依赖捕捉能力**：任意两个token只需一次注意力计算就能完成信息交互，不管序列多长，长距离依赖的建模难度不会增加，彻底解决了RNN的长序列痛点。
2. **完美的并行计算能力**：训练阶段全序列并行，充分利用GPU的并行算力，支持大规模数据和大参数量模型的训练。
3. **强大的特征表达能力**：多头注意力可以同时捕捉多种类型的语义、语法、依赖关系，配合非线性FFN和深层结构，特征拟合能力极强。
4. **优秀的泛化能力**：Transformer的架构通用性极强，不仅适配NLP的所有任务，还可以扩展到语音、图像、视频等多模态领域，成为AI领域的通用基础架构。



## 八、Transformer demo
用到了英语-德语翻译任务，数据集为Multi30k
```
import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
import json
import numpy as np
from tqdm import tqdm
import nltk
from nltk.tokenize import word_tokenize
import random


# -------------------------- 基础配置 --------------------------
# 适配你的 5060ti 8G GPU
DEVICE = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
print(f"使用设备: {DEVICE}")

# 数据集路径（你提供的路径）
TRAIN_FILE_PATH = "/home/wuyi/code/dl/data/Multi30k/train.jsonl"
VAL_FILE_PATH = "/home/wuyi/code/dl/data/Multi30k/val.jsonl"
TEST_FILE_PATH = "/home/wuyi/code/dl/data/Multi30k/test.jsonl"

# 超参数（轻量化，适配 8G GPU）
EMBEDDING_DIM = 256    # 嵌入维度
NUM_HEADS = 4          # 多头注意力头数
FFN_HIDDEN_DIM = 512   # 前馈网络隐藏层维度
NUM_ENCODER_LAYERS = 2 # 编码器层数
NUM_DECODER_LAYERS = 2 # 解码器层数
MAX_SEQ_LEN = 50       # 最大序列长度
BATCH_SIZE = 32        # 批次大小
EPOCHS = 10            # 训练轮数
LEARNING_RATE = 1e-3   # 学习率

# 特殊标记
PAD_TOKEN = "<PAD>"    # 填充标记
SOS_TOKEN = "<SOS>"    # 起始标记
EOS_TOKEN = "<EOS>"    # 结束标记
UNK_TOKEN = "<UNK>"    # 未知标记

# -------------------------- 1. 数据预处理 --------------------------
# 下载nltk分词器（首次运行需要）
# nltk.download('punkt')

class Vocabulary:
    """词汇表类：将单词转为索引，索引转为单词"""
    def __init__(self):
        self.word2idx = {
            PAD_TOKEN: 0,
            SOS_TOKEN: 1,
            EOS_TOKEN: 2,
            UNK_TOKEN: 3
        }
        self.idx2word = {v: k for k, v in self.word2idx.items()}
        self.word_count = 4  # 初始4个特殊标记
    
    def add_word(self, word):
        """添加单词到词汇表"""
        if word not in self.word2idx:
            self.word2idx[word] = self.word_count
            self.idx2word[self.word_count] = word
            self.word_count += 1
    
    def add_sentence(self, sentence):
        """添加整句话到词汇表"""
        for word in word_tokenize(sentence.lower()):
            self.add_word(word)
    
    def __len__(self):
        return self.word_count

class Multi30kDataset(Dataset):
    """Multi30k数据集类"""
    def __init__(self, file_path, src_vocab=None, tgt_vocab=None, build_vocab=True):
        self.data = []
        # 读取jsonl文件
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                item = json.loads(line.strip())
                # 假设数据格式是 {"en": "英语句子", "de": "德语句子"}
                # 如果你的数据格式不同，需要调整这里的key
                src_sentence = item.get("en", "")
                tgt_sentence = item.get("de", "")
                self.data.append((src_sentence, tgt_sentence))
        
        # 构建词汇表
        self.src_vocab = src_vocab if src_vocab else Vocabulary()
        self.tgt_vocab = tgt_vocab if tgt_vocab else Vocabulary()
        
        if build_vocab:
            for src_sent, tgt_sent in self.data:
                self.src_vocab.add_sentence(src_sent)
                self.tgt_vocab.add_sentence(tgt_sent)
    
    def __len__(self):
        return len(self.data)
    
    def sentence_to_tensor(self, sentence, vocab):
        """将句子转为张量（添加SOS/EOS，填充到最大长度）"""
        # 分词 + 转索引
        tokens = [SOS_TOKEN] + word_tokenize(sentence.lower()) + [EOS_TOKEN]
        indices = [vocab.word2idx.get(token, vocab.word2idx[UNK_TOKEN]) for token in tokens]
        
        # 填充到最大长度
        if len(indices) < MAX_SEQ_LEN:
            indices += [vocab.word2idx[PAD_TOKEN]] * (MAX_SEQ_LEN - len(indices))
        else:
            indices = indices[:MAX_SEQ_LEN]  # 截断
        
        return torch.tensor(indices, dtype=torch.long)
    
    def __getitem__(self, idx):
        src_sent, tgt_sent = self.data[idx]
        src_tensor = self.sentence_to_tensor(src_sent, self.src_vocab)
        tgt_tensor = self.sentence_to_tensor(tgt_sent, self.tgt_vocab)
        return src_tensor, tgt_tensor

# 构建词汇表 + 加载数据集
print("加载数据集并构建词汇表...")
train_dataset = Multi30kDataset(TRAIN_FILE_PATH)
src_vocab = train_dataset.src_vocab  # 英语词汇表
tgt_vocab = train_dataset.tgt_vocab  # 德语词汇表


# 验证/测试集复用训练集的词汇表
val_dataset = Multi30kDataset(VAL_FILE_PATH, src_vocab, tgt_vocab, build_vocab=False)
test_dataset = Multi30kDataset(TEST_FILE_PATH, src_vocab, tgt_vocab, build_vocab=False)

# 数据加载器
train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)
val_loader = DataLoader(val_dataset, batch_size=BATCH_SIZE, shuffle=False)
test_loader = DataLoader(test_dataset, batch_size=BATCH_SIZE, shuffle=False)

print(f"英语词汇表大小: {len(src_vocab)}")
print(f"德语词汇表大小: {len(tgt_vocab)}")

# -------------------------- 2. Transformer 核心组件 --------------------------
class PositionalEncoding(nn.Module):
    """位置编码：给序列添加位置信息"""
    def __init__(self, embedding_dim, max_len=MAX_SEQ_LEN):
        super().__init__()
        # 计算位置编码 
        pe = torch.zeros(max_len, embedding_dim) # [max_len, embedding_dim]
        position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)  # [max_len, 1]
        div_term = torch.exp(torch.arange(0, embedding_dim, 2).float() * (-np.log(10000.0) / embedding_dim))
        
        pe[:, 0::2] = torch.sin(position * div_term)  # 偶数维度用sin
        pe[:, 1::2] = torch.cos(position * div_term)  # 奇数维度用cos
        pe = pe.unsqueeze(0)  # [1, max_len, embedding_dim] -> [1, 50, 256]
        self.register_buffer('pe', pe)  # 不参与训练的参数
    
    def forward(self, x):
        """x: [batch_size, seq_len, embedding_dim]"""
        x = x + self.pe[:, :x.size(1), :].to(x.device)
        return x

class MultiHeadAttention(nn.Module):
    """多头注意力机制"""
    def __init__(self, embedding_dim, num_heads):
        super().__init__()
        assert embedding_dim % num_heads == 0, "嵌入维度必须能被头数整除"
        
        self.embedding_dim = embedding_dim
        self.num_heads = num_heads
        self.head_dim = embedding_dim // num_heads
        
        # 线性层：Q/K/V 投影
        self.q_linear = nn.Linear(embedding_dim, embedding_dim)
        self.k_linear = nn.Linear(embedding_dim, embedding_dim)
        self.v_linear = nn.Linear(embedding_dim, embedding_dim)
        
        # 输出线性层
        self.out_linear = nn.Linear(embedding_dim, embedding_dim)
        
        # 缩放因子
        self.scale = torch.sqrt(torch.FloatTensor([self.head_dim])).to(DEVICE)
    
    def forward(self, q, k, v, mask=None):
        """
        q: [batch_size, q_len, embedding_dim]
        k: [batch_size, k_len, embedding_dim]
        v: [batch_size, v_len, embedding_dim]
        mask: [batch_size, 1, q_len, k_len] （可选）
        """
        batch_size = q.size(0)
        
        # 投影到多个头
        Q = self.q_linear(q)  # [batch_size, q_len, embedding_dim] → [32, seq_len, 256]
        K = self.k_linear(k)  # [batch_size, k_len, embedding_dim] → [32, seq_len, 256]
        V = self.v_linear(v)  # [batch_size, v_len, embedding_dim] → [32, seq_len, 256]
        
        # 拆分多头：[batch_size, num_heads, seq_len, head_dim]
        # [32, seq_len, 256] → view → [32, seq_len, 4, 64] → permute → [32, 4, seq_len, 64]
        Q = Q.view(batch_size, -1, self.num_heads, self.head_dim).permute(0, 2, 1, 3)
        K = K.view(batch_size, -1, self.num_heads, self.head_dim).permute(0, 2, 1, 3)
        V = V.view(batch_size, -1, self.num_heads, self.head_dim).permute(0, 2, 1, 3)
        
        # 计算注意力分数：Q @ K^T / scale
        # Q @ K^T: [32, 4, seq_len, 64] @ [32, 4, 64, seq_len] = [32, 4, seq_len, seq_len]
        attention_scores = torch.matmul(Q, K.permute(0, 1, 3, 2)) / self.scale  # [batch_size, num_heads, q_len, k_len]
        
        # 应用mask（填充mask或未来mask）
        if mask is not None:
            attention_scores = attention_scores.masked_fill(mask == 0, -1e10)
        
        # 计算注意力权重
        attention_weights = F.softmax(attention_scores, dim=-1) # [32, 4, seq_len, seq_len]
        
        # 加权求和
        # [32, 4, seq_len, seq_len] @ [32, 4, seq_len, 64] = [32, 4, seq_len, 64]
        output = torch.matmul(attention_weights, V)
        
        # 拼接多头
        output = output.permute(0, 2, 1, 3).contiguous()  # [32, seq_len, 4, 64]
        output = output.view(batch_size, -1, self.embedding_dim)   # [32, seq_len, 4, 64] → [32, seq_len, 256]
        
        # 输出投影
        output = self.out_linear(output) # [32, seq_len, 256] → [32, seq_len, 256]
        
        return output, attention_weights

class FeedForwardNetwork(nn.Module):
    """前馈网络：两层线性 + ReLU"""
    def __init__(self, embedding_dim, hidden_dim):
        super().__init__()
        self.linear1 = nn.Linear(embedding_dim, hidden_dim)
        self.linear2 = nn.Linear(hidden_dim, embedding_dim)
        self.relu = nn.ReLU()
    
    def forward(self, x):
        """x: [batch_size, seq_len, embedding_dim]"""
        return self.linear2(self.relu(self.linear1(x)))

class EncoderLayer(nn.Module):
    """编码器单层：多头自注意力 + 前馈网络 + 残差连接 + 层归一化"""
    def __init__(self, embedding_dim, num_heads, ffn_hidden_dim):
        super().__init__()
        self.self_attn = MultiHeadAttention(embedding_dim, num_heads)
        self.ffn = FeedForwardNetwork(embedding_dim, ffn_hidden_dim)
        
        # 层归一化
        self.norm1 = nn.LayerNorm(embedding_dim)
        self.norm2 = nn.LayerNorm(embedding_dim)
        
        # Dropout（可选，增加鲁棒性）
        self.dropout = nn.Dropout(0.1)
    
    def forward(self, x, src_mask):
        """
        x: [batch_size, src_len, embedding_dim]
        src_mask: [batch_size, 1, src_len, src_len]
        """
        # 自注意力 + 残差连接 + 层归一化
        attn_output, _ = self.self_attn(x, x, x, src_mask)
        x = self.norm1(x + self.dropout(attn_output))
        
        # 前馈网络 + 残差连接 + 层归一化
        ffn_output = self.ffn(x)
        x = self.norm2(x + self.dropout(ffn_output))
        
        return x

class DecoderLayer(nn.Module):
    """解码器单层：掩码自注意力 + 编码器-解码器注意力 + 前馈网络"""
    def __init__(self, embedding_dim, num_heads, ffn_hidden_dim):
        super().__init__()
        self.masked_self_attn = MultiHeadAttention(embedding_dim, num_heads)  # 掩码自注意力
        self.enc_dec_attn = MultiHeadAttention(embedding_dim, num_heads)      # 编码器-解码器注意力
        self.ffn = FeedForwardNetwork(embedding_dim, ffn_hidden_dim)
        
        # 层归一化
        self.norm1 = nn.LayerNorm(embedding_dim)
        self.norm2 = nn.LayerNorm(embedding_dim)
        self.norm3 = nn.LayerNorm(embedding_dim)
        
        self.dropout = nn.Dropout(0.1)
    
    def forward(self, x, enc_output, tgt_mask, src_tgt_mask):
        """
        x: [batch_size, tgt_len, embedding_dim]
        enc_output: [batch_size, src_len, embedding_dim]
        tgt_mask: [batch_size, 1, tgt_len, tgt_len] （未来掩码）
        src_tgt_mask: [batch_size, 1, tgt_len, src_len] （填充掩码）
        """
        # 1. 掩码自注意力
        attn1_output, _ = self.masked_self_attn(x, x, x, tgt_mask)
        x = self.norm1(x + self.dropout(attn1_output))
        
        # 2. 编码器-解码器注意力
        attn2_output, _ = self.enc_dec_attn(x, enc_output, enc_output, src_tgt_mask)
        x = self.norm2(x + self.dropout(attn2_output))
        
        # 3. 前馈网络
        ffn_output = self.ffn(x)
        x = self.norm3(x + self.dropout(ffn_output))
        
        return x

class Transformer(nn.Module):
    """完整的Transformer模型"""
    """ src_vocab_size:10218, 
        tgt_vocab_size:18679, 
        embedding_dim:256, 
        num_heads:4, 
        ffn_hidden_dim:512, 
        num_encoder_layers:2, 
        num_decoder_layers:2
        """
    def __init__(self, src_vocab_size, tgt_vocab_size, embedding_dim, num_heads, 
                 ffn_hidden_dim, num_encoder_layers, num_decoder_layers): 
        super().__init__()
        
        # 嵌入层
        self.src_embedding = nn.Embedding(src_vocab_size, embedding_dim) # [src_vocab_size, 256] -> [10218, 256]
        self.tgt_embedding = nn.Embedding(tgt_vocab_size, embedding_dim) # [dst_vocab_size, 256] -> [18679, 256]
        
        # 位置编码
        self.positional_encoding = PositionalEncoding(embedding_dim) # [1, 50, 256]
        
        # 编码器
        self.encoder_layers = nn.ModuleList([
            EncoderLayer(embedding_dim, num_heads, ffn_hidden_dim) 
            for _ in range(num_encoder_layers)
        ])
        
        # 解码器
        self.decoder_layers = nn.ModuleList([
            DecoderLayer(embedding_dim, num_heads, ffn_hidden_dim) 
            for _ in range(num_decoder_layers)
        ])
        
        # 输出层（映射到目标词汇表）
        self.fc_out = nn.Linear(embedding_dim, tgt_vocab_size)
        
        # Dropout
        self.dropout = nn.Dropout(0.1)
    
    def create_src_mask(self, src):
        """创建源序列掩码（屏蔽PADToken）"""
        # src: [batch_size, src_len]
        src_mask = (src != src_vocab.word2idx[PAD_TOKEN]).unsqueeze(1).unsqueeze(2)
        # src_mask: [batch_size, 1, 1, src_len]
        return src_mask.to(DEVICE)
    
    def create_tgt_mask(self, tgt):
        """创建目标序列掩码（屏蔽PADToken + 未来信息）"""
        # tgt: [batch_size, tgt_len]
        batch_size, tgt_len = tgt.shape
        
        # 1. 填充掩码
        tgt_pad_mask = (tgt != tgt_vocab.word2idx[PAD_TOKEN]).unsqueeze(1).unsqueeze(2)
        # tgt_pad_mask: [batch_size, 1, 1, tgt_len]
        
        # 2. 未来掩码（上三角矩阵，屏蔽未来token）
        tgt_subsequent_mask = torch.tril(torch.ones((tgt_len, tgt_len), device=DEVICE)).bool()
        # tgt_subsequent_mask: [tgt_len, tgt_len]
        
        # 合并掩码
        tgt_mask = tgt_pad_mask & tgt_subsequent_mask
        # tgt_mask: [batch_size, 1, tgt_len, tgt_len]
        return tgt_mask.to(DEVICE)
    
    def forward(self, src, tgt):
        """
        src: [batch_size, src_len]
        tgt: [batch_size, tgt_len]
        """
        # 1. 创建掩码
        src_mask = self.create_src_mask(src)
        tgt_mask = self.create_tgt_mask(tgt)
        src_tgt_mask = self.create_src_mask(src)  # 编码器-解码器注意力的掩码
        
        # 2. 源序列编码
        src_emb = self.dropout(self.positional_encoding(self.src_embedding(src)))
        enc_output = src_emb
        for enc_layer in self.encoder_layers:
            enc_output = enc_layer(enc_output, src_mask)
        
        # 3. 目标序列解码
        tgt_emb = self.dropout(self.positional_encoding(self.tgt_embedding(tgt)))
        dec_output = tgt_emb
        for dec_layer in self.decoder_layers:
            dec_output = dec_layer(dec_output, enc_output, tgt_mask, src_tgt_mask)
        
        # 4. 输出投影
        output = self.fc_out(dec_output)
        
        return output

# -------------------------- 3. 训练和测试 --------------------------
# 初始化模型
model = Transformer(
    src_vocab_size=len(src_vocab),
    tgt_vocab_size=len(tgt_vocab),
    embedding_dim=EMBEDDING_DIM,
    num_heads=NUM_HEADS,
    ffn_hidden_dim=FFN_HIDDEN_DIM,
    num_encoder_layers=NUM_ENCODER_LAYERS,
    num_decoder_layers=NUM_DECODER_LAYERS
).to(DEVICE)

# 损失函数（忽略PADToken）
criterion = nn.CrossEntropyLoss(ignore_index=src_vocab.word2idx[PAD_TOKEN])

# 优化器
optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE)

def train_epoch(model, loader, optimizer, criterion):
    """训练一轮"""
    model.train()
    total_loss = 0.0
    
    for batch_idx, (src_batch, tgt_batch) in enumerate(tqdm(loader, desc="训练")):
        src_batch = src_batch.to(DEVICE)
        tgt_batch = tgt_batch.to(DEVICE)
        
        # 清零梯度
        optimizer.zero_grad()
        
        # 输入：tgt_batch[:, :-1]（去掉最后一个token）
        # 目标：tgt_batch[:, 1:]（去掉第一个token）
        #   为什么要错开一位？

        #   Decoder 输入 ([:-1]):  <SOS>   ich   liebe  dich
        #                             ↓      ↓      ↓      ↓
        #   预测目标 ([1:]):       ich    liebe  dich   <EOS>

        #   输入和目标正好错开一位——这就是 "shift right" 的本质：

        #   - 输入第 0 个词 <SOS>，模型应该输出第 1 个词 ich
        #   - 输入第 1 个词 ich（已知道），模型应该输出第 2 个词 liebe
        #   - 以此类推...
        output = model(src_batch, tgt_batch[:, :-1])    # 维度说明：src_batch: [32, 50], tgt_batch[:, :-1]: [32, 49]， output: [batch_size, tgt_len-1, tgt_vocab_size]  → [32, 49, vocab_size] 
        
        # 调整形状计算损失
        output = output.reshape(-1, output.shape[-1])   # [32*49, vocab_size]
        target = tgt_batch[:, 1:].reshape(-1)           # [32*49]
        
        # 计算损失
        loss = criterion(output, target)
        
        # 反向传播
        loss.backward()
        
        # 梯度裁剪（防止梯度爆炸）
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        
        # 更新参数
        optimizer.step()
        
        total_loss += loss.item()
    
    return total_loss / len(loader)

def evaluate(model, loader, criterion):
    """验证/测试"""
    model.eval()
    total_loss = 0.0
    
    with torch.no_grad():
        for batch_idx, (src_batch, tgt_batch) in enumerate(tqdm(loader, desc="验证")):
            src_batch = src_batch.to(DEVICE)
            tgt_batch = tgt_batch.to(DEVICE)
            
            output = model(src_batch, tgt_batch[:, :-1])
            output = output.reshape(-1, output.shape[-1])
            target = tgt_batch[:, 1:].reshape(-1)
            
            loss = criterion(output, target)
            total_loss += loss.item()
    
    return total_loss / len(loader)

def translate_sentence(model, sentence, src_vocab, tgt_vocab, max_len=MAX_SEQ_LEN):
    """单句翻译（推理）"""
    model.eval()
    
    # 预处理输入句子
    tokens = [SOS_TOKEN] + word_tokenize(sentence.lower()) + [EOS_TOKEN]
    src_indices = [src_vocab.word2idx.get(token, src_vocab.word2idx[UNK_TOKEN]) for token in tokens]
    src_tensor = torch.tensor(src_indices, dtype=torch.long).unsqueeze(0).to(DEVICE)
    
    # 初始化目标序列（仅包含SOS_TOKEN）
    tgt_indices = [tgt_vocab.word2idx[SOS_TOKEN]]
    tgt_tensor = torch.tensor(tgt_indices, dtype=torch.long).unsqueeze(0).to(DEVICE)
    
    with torch.no_grad():
        for _ in range(max_len):
            # 前向传播
            output = model(src_tensor, tgt_tensor)
            
            # 取最后一个token的预测
            next_token_logits = output[:, -1, :]
            next_token_idx = torch.argmax(next_token_logits, dim=-1).item()
            
            # 添加到目标序列
            tgt_indices.append(next_token_idx)
            
            # 更新目标张量
            tgt_tensor = torch.tensor(tgt_indices, dtype=torch.long).unsqueeze(0).to(DEVICE)
            
            # 如果预测到EOS_TOKEN，停止
            if next_token_idx == tgt_vocab.word2idx[EOS_TOKEN]:
                break
    
    # 转换为单词
    translated_words = [tgt_vocab.idx2word[idx] for idx in tgt_indices if idx not in 
                        [tgt_vocab.word2idx[SOS_TOKEN], tgt_vocab.word2idx[EOS_TOKEN], 
                         tgt_vocab.word2idx[PAD_TOKEN]]]
    
    return ' '.join(translated_words)

# 开始训练
print("\n开始训练...")
best_val_loss = float('inf')

for epoch in range(EPOCHS):
    # 训练
    train_loss = train_epoch(model, train_loader, optimizer, criterion)
    
    # 验证
    val_loss = evaluate(model, val_loader, criterion)
    
    print(f"\nEpoch {epoch+1}/{EPOCHS}")
    print(f"训练损失: {train_loss:.4f}")
    print(f"验证损失: {val_loss:.4f}")
    
    # 保存最佳模型
    if val_loss < best_val_loss:
        best_val_loss = val_loss
        torch.save(model.state_dict(), "transformer_eng2de_best.pth")
        print("保存最佳模型！")

# 测试
print("\n开始测试...")
test_loss = evaluate(model, test_loader, criterion)
print(f"测试损失: {test_loss:.4f}")

# 示例翻译
print("\n示例翻译：")
test_sentences = [
    "A man is riding a horse.",
    "Two dogs are playing in the park.",
    "I love you."
]

for sentence in test_sentences:
    translation = translate_sentence(model, sentence, src_vocab, tgt_vocab)
    print(f"英语: {sentence}")
    print(f"德语: {translation}")
    print("-" * 50)
```
