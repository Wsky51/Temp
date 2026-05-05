# transfomer网络结果实现（手写）

```
import torch
import torch.nn as nn
import math


# """缩放点积注意力机制"""
class ScaledDotProductAttention(nn.Module):
    def __init__(self, dropout=0.1):
        super().__init__()
        self.dropout = nn.Dropout(dropout)

    def forward(
        self,
        Q: torch.Tensor,
        K: torch.Tensor,
        V: torch.Tensor,
        mask: torch.Tensor = None,
    ) -> torch.Tensor:
        scores = Q.matmul(K.transpose(-2, -1)) / math.sqrt(
            Q.size(-1)
        )  # 缩放点积，计算相似度
        if mask is not None:
            scores = scores.masked_fill(mask == 0, float("-inf"))
        attn_weights = torch.softmax(scores, dim=-1)
        attn_weights = self.dropout(attn_weights)
        output = attn_weights.matmul(V)
        return output


class FeedForwardNetwork(nn.Module):
    def __init__(self, dim, dropout):
        """初始化"""
        super().__init__()
        self.layer1 = nn.Linear(dim, dim * 4)
        self.gelu = nn.GELU()
        self.dropout1 = nn.Dropout(dropout)
        self.layer2 = nn.Linear(dim * 4, dim)

    def forward(self, x):
        output = self.layer2(self.dropout1(self.gelu(self.layer1(x))))
        return output


class MultiHeadAttention(nn.Module):
    def __init__(self, dim, heads, dropout=0.1):
        """初始化"""
        super().__init__()
        self.dim = dim
        self.heads = heads
        self.head_dim = dim // heads
        assert dim % heads == 0

        # wq, wk, wv权重参数
        self.wq = nn.Linear(self.dim, self.dim)
        self.wk = nn.Linear(self.dim, self.dim)
        self.wv = nn.Linear(self.dim, self.dim)
        self.wo = nn.Linear(self.dim, self.dim)

        # 缩放点积注意力
        self.attn = ScaledDotProductAttention(dropout=dropout)

    def forward(
        self, q: torch.Tensor, kv: torch.Tensor = None, mask: torch.Tensor = None
    ):
        if kv is None:
            kv = q
        batch, seq_num, dim = q.shape

        assert dim == self.dim

        Q = (
            self.wq(q)
            .reshape(q.shape[0], q.shape[1], self.heads, self.head_dim)
            .permute(0, 2, 1, 3)
        )
        K = (
            self.wk(kv)
            .reshape(kv.shape[0], kv.shape[1], self.heads, self.head_dim)
            .permute(0, 2, 1, 3)
        )
        V = (
            self.wv(kv)
            .reshape(kv.shape[0], kv.shape[1], self.heads, self.head_dim)
            .permute(0, 2, 1, 3)
        )
        output = self.attn(Q, K, V, mask)
        output = output.permute(0, 2, 1, 3).reshape(batch, seq_num, dim)
        return self.wo(output)


# （Pre-LN 结构）
class EncoderLayer(nn.Module):
    def __init__(self, dim, heads, dropout=0.1):
        super().__init__()
        self.norm1 = nn.LayerNorm(dim)
        self.attn = MultiHeadAttention(dim, heads, dropout)
        self.norm2 = nn.LayerNorm(dim)
        self.ffn = FeedForwardNetwork(dim, dropout)
        self.dropout1 = nn.Dropout(dropout)
        self.dropout2 = nn.Dropout(dropout)

    def forward(self, x, mask=None):
        x = x + self.dropout1(self.attn(self.norm1(x), mask=mask))
        x = x + self.dropout2(self.ffn(self.norm2(x)))
        return x


class Encoder(nn.Module):
    def __init__(self, dim, heads, layers=6, dropout=0.1):
        super().__init__()
        self.layers = nn.ModuleList(
            [EncoderLayer(dim, heads, dropout) for _ in range(layers)]
        )
        self.norm = nn.LayerNorm(dim)

    def forward(self, x, mask=None):
        for layer in self.layers:
            x = layer(x, mask=mask)
        return self.norm(x)


class DecoderLayer(nn.Module):
    def __init__(self, dim, heads, dropout=0.1):
        super().__init__()
        self.norm1 = nn.LayerNorm(dim)
        self.norm2 = nn.LayerNorm(dim)
        self.norm3 = nn.LayerNorm(dim)

        self.self_attn = MultiHeadAttention(dim, heads, dropout)
        self.cross_attn = MultiHeadAttention(dim, heads, dropout)
        self.ffn = FeedForwardNetwork(dim, dropout)

        self.dropout1 = nn.Dropout(dropout)
        self.dropout2 = nn.Dropout(dropout)
        self.dropout3 = nn.Dropout(dropout)

    def forward(self, x, enc_output, self_mask=None, cross_mask=None):
        x = x + self.dropout1(
            self.self_attn(self.norm1(x), mask=self_mask)
        )  # 先自注意力
        x = x + self.dropout2(
            self.cross_attn(self.norm2(x), enc_output, mask=cross_mask)
        )  # 再交叉自注意力
        x = x + self.dropout3(self.ffn(self.norm3(x)))  # 最后一层ffn
        return x


class Decoder(nn.Module):
    def __init__(self, dim, heads, layers=6, dropout=0.1):
        super().__init__()
        self.layers = nn.ModuleList(
            [DecoderLayer(dim, heads, dropout) for _ in range(layers)]
        )
        self.norm = nn.LayerNorm(dim)

    def forward(self, x, enc_output, self_mask, cross_mask):
        for layer in self.layers:
            x = layer(x, enc_output, self_mask, cross_mask)
        return self.norm(x)


class PositionalEncoding(nn.Module):
    def __init__(self, dim, max_len=5000, dropout=0.1):
        super().__init__()
        self.dropout = nn.Dropout(dropout)

        pe = torch.zeros(max_len, dim)  # 最终生成的维度

        position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
        div_term = torch.exp(
            torch.arange(0, dim, 2, dtype=torch.float) * (-math.log(10000.0) / dim)
        )
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)

        # 添加 batch 维度: (1, max_len, dim)
        pe = pe.unsqueeze(0)

        self.register_buffer("pe", pe)

    def forward(self, x):
        return self.dropout(x + self.pe[:, : x.size(1)])


class Transformer(nn.Module):
    def __init__(
        self,
        src_vocab_size,
        tgt_vocab_size,
        dim,
        heads,
        enc_layers=6,
        dec_layers=6,
        dropout=0.1,
        max_len=5000,
    ):
        super().__init__()
        self.dim = dim
        self.src_embed = nn.Embedding(src_vocab_size, dim)
        self.dst_embed = nn.Embedding(tgt_vocab_size, dim)
        self.pe = PositionalEncoding(dim, max_len, dropout)
        self.encoder = Encoder(dim, heads, enc_layers, dropout)
        self.decoder = Decoder(dim, heads, dec_layers, dropout)
        self.output_proj = nn.Linear(dim, tgt_vocab_size)
        for p in self.parameters():
            if p.dim() > 1:
                nn.init.xavier_uniform_(p)

    def _encode(self, src, src_mask):
        return self.encoder(self.pe(self.src_embed(src) * math.sqrt(self.dim)), src_mask)

    def _decode_step(self, tgt, enc_output, tgt_mask, src_mask):
        dec_output = self.decoder(
            self.pe(self.dst_embed(tgt) * math.sqrt(self.dim)),
            enc_output, tgt_mask, src_mask,
        )
        return self.output_proj(dec_output)

    def forward(self, src, tgt, src_mask=None, tgt_mask=None):
        enc_output = self._encode(src, src_mask)
        return self._decode_step(tgt, enc_output, tgt_mask, src_mask)

    @torch.no_grad()
    def generate(self, src, src_mask, sos_id, eos_id, max_len=50):
        """贪心解码，返回 (batch, seq_len) 的 token id"""
        self.eval()
        enc_output = self._encode(src, src_mask)
        batch_size = src.size(0)
        tgt = torch.full((batch_size, 1), sos_id, dtype=torch.long, device=src.device)

        for _ in range(max_len - 1):
            tgt_mask = generate_decoder_self_mask(tgt)
            logits = self._decode_step(tgt, enc_output, tgt_mask, src_mask)
            next_id = logits[:, -1, :].argmax(dim=-1, keepdim=True)
            tgt = torch.cat([tgt, next_id], dim=1)
            if (next_id == eos_id).all():
                break
        return tgt


def generate_padding_mask(seq, pad_id=0):                                                                                                                                                                                
    """生成padding掩码，pad位置为0，有效位置为1                                                                                                                                                                          
    seq: (batch, seq_len)                                                                                                                                                                                                
    返回: (batch, 1, 1, seq_len) — 方便广播到 (batch, heads, seq_q, seq_k)                                                                                                                                               
    """                                                                                                                                                                                                                  
    return (seq != pad_id).unsqueeze(1).unsqueeze(2) 


def generate_causal_mask(seq_len, device=None):
    """生成因果掩码，下三角为1，上三角为0
    返回: (1, 1, seq_len, seq_len) — 方便广播到batch和heads维度
    """
    return torch.tril(torch.ones(seq_len, seq_len, device=device)).unsqueeze(0).unsqueeze(0)


def generate_decoder_self_mask(tgt, pad_id=0):                                                                                                                                                                           
    """生成decoder self-attention的组合掩码 = causal AND tgt_padding                                                                                                                                                     
    tgt: (batch, tgt_len)                                                                                                                                                                                                
    返回: (batch, 1, tgt_len, tgt_len)                                                                                                                                                                                   
    """                                                                                                                                                                                                                  
    tgt_pad_mask = (tgt != pad_id).unsqueeze(1).unsqueeze(2)  # (batch, 1, 1, tgt_len)                                                                                                                                   
    causal_mask = torch.tril(torch.ones(tgt.size(1), tgt.size(1), device=tgt.device)).unsqueeze(0).unsqueeze(0)  # (1, 1, tgt_len, tgt_len)                                                                                                 
    return (tgt_pad_mask & causal_mask.bool()).float() 

if __name__ == "__main__":
    trans = Transformer(12000, 18000, 128, 4)
    src = torch.randint(0, 12000, (8, 15))
    dst = torch.randint(0, 18000, (8, 20))
    out = trans(src, dst)
    print(f"out shape: {out.shape}")

    # # 批次大小=2，序列长度=5，0 是 padding
    # test_seq = torch.tensor([[1, 2, 3, 0, 0], [4, 5, 0, 0, 0]])

    # # 1. 测试 Padding 掩码
    # pad_mask = generate_padding_mask(test_seq)
    # print("Padding 掩码形状:", pad_mask.shape)
    # print("Padding 掩码:\n", pad_mask)

    # # 2. 测试 causal 掩码
    # causal_mask = generate_causal_mask(3)
    # print("Padding 掩码形状:", causal_mask.shape)
    # print("Padding 掩码:\n", causal_mask)

    # # 3. 测试 combine 掩码
    # combine_mask = generate_decoder_self_mask(test_seq)
    # print("Padding 掩码形状:", combine_mask.shape)
    # print("Padding 掩码:\n", combine_mask)
```



# train_multi30k 网络结果实现（手写）
```
import json
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from torch.nn.utils.rnn import pad_sequence
from collections import Counter
from my_transformer import (
    Transformer,
    generate_padding_mask,
    generate_decoder_self_mask,
)

# ── 特殊 token ──────────────────────────────────────────
PAD_ID = 0
SOS_ID = 1
EOS_ID = 2
UNK_ID = 3


# ── 词表 ────────────────────────────────────────────────
class Vocabulary:
    def __init__(self):
        self.word2idx = {"<pad>": PAD_ID, "<sos>": SOS_ID, "<eos>": EOS_ID, "<unk>": UNK_ID}
        self.idx2word = {v: k for k, v in self.word2idx.items()}
        self.counter = Counter()

    def add_sentence(self, sentence):
        for word in sentence.lower().split():
            self.counter[word] += 1

    def build(self, min_freq=2):
        for word, freq in sorted(self.counter.items()):
            if freq >= min_freq:
                idx = len(self.word2idx)
                self.word2idx[word] = idx
                self.idx2word[idx] = word

    def encode(self, sentence):
        return [SOS_ID, *(self.word2idx.get(w, UNK_ID) for w in sentence.lower().split()), EOS_ID]

    def decode(self, ids):
        tokens = []
        for i in ids:
            if i in (PAD_ID, SOS_ID):
                continue
            if i == EOS_ID:
                break
            tokens.append(self.idx2word.get(i, "<unk>"))
        return " ".join(tokens)

    def __len__(self):
        return len(self.word2idx)


# ── 数据集 ──────────────────────────────────────────────
class TranslationDataset(Dataset):
    def __init__(self, filepath, src_vocab, tgt_vocab):
        self.pairs = []
        with open(filepath) as f:
            for line in f:
                d = json.loads(line)
                self.pairs.append((src_vocab.encode(d["en"]), tgt_vocab.encode(d["de"])))

    def __len__(self):
        return len(self.pairs)

    def __getitem__(self, idx):
        return torch.tensor(self.pairs[idx][0]), torch.tensor(self.pairs[idx][1])


def collate_fn(batch):
    srcs, tgts = zip(*batch)
    srcs = pad_sequence(srcs, batch_first=True, padding_value=PAD_ID)
    tgts = pad_sequence(tgts, batch_first=True, padding_value=PAD_ID)
    return srcs, tgts[:, :-1], tgts[:, 1:]


# ── 训练 / 验证 ────────────────────────────────────────
def train_epoch(model, dataloader, optimizer, criterion, scheduler, device):
    model.train()
    total_loss = 0
    for src, tgt_input, tgt_output in dataloader:
        src, tgt_input, tgt_output = src.to(device), tgt_input.to(device), tgt_output.to(device)

        src_mask = generate_padding_mask(src).to(device)
        tgt_mask = generate_decoder_self_mask(tgt_input).to(device)

        logits = model(src, tgt_input, src_mask=src_mask, tgt_mask=tgt_mask)
        loss = criterion(logits.reshape(-1, logits.size(-1)), tgt_output.reshape(-1))

        optimizer.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        optimizer.step()
        scheduler.step()
        total_loss += loss.item()
    return total_loss / len(dataloader)


@torch.no_grad()
def evaluate(model, dataloader, criterion, device):
    model.eval()
    total_loss = 0
    for src, tgt_input, tgt_output in dataloader:
        src, tgt_input, tgt_output = src.to(device), tgt_input.to(device), tgt_output.to(device)

        src_mask = generate_padding_mask(src).to(device)
        tgt_mask = generate_decoder_self_mask(tgt_input).to(device)

        logits = model(src, tgt_input, src_mask=src_mask, tgt_mask=tgt_mask)
        loss = criterion(logits.reshape(-1, logits.size(-1)), tgt_output.reshape(-1))
        total_loss += loss.item()
    return total_loss / len(dataloader)


# ── 贪心翻译 ────────────────────────────────────────────
@torch.no_grad()
def translate(model, sentence, src_vocab, tgt_vocab, device, max_len=50):
    model.eval()
    src_ids = torch.tensor([src_vocab.encode(sentence)], device=device)
    src_mask = generate_padding_mask(src_ids).to(device)
    output_ids = model.generate(src_ids, src_mask, SOS_ID, EOS_ID, max_len)
    return tgt_vocab.decode(output_ids[0].tolist())


# ── 词表构建工具 ────────────────────────────────────────
def build_vocabularies(data_dir, min_freq):
    en_vocab, de_vocab = Vocabulary(), Vocabulary()
    with open(f"{data_dir}/train.jsonl") as f:
        for line in f:
            d = json.loads(line)
            en_vocab.add_sentence(d["en"])
            de_vocab.add_sentence(d["de"])
    en_vocab.build(min_freq)
    de_vocab.build(min_freq)
    return en_vocab, de_vocab


# ── 主流程 ──────────────────────────────────────────────
if __name__ == "__main__":
    # 超参数（适配 Multi30k 小数据集）
    dim = 256
    heads = 4
    enc_layers = 4
    dec_layers = 4
    dropout = 0.1
    batch_size = 128
    lr = 5e-4
    epochs = 30
    warmup_steps = 2000
    max_len = 50
    min_freq = 2
    data_dir = "data/Multi30k"
    save_path = "transformer_en_de.pt"
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # 词表
    print("Building vocabularies...")
    en_vocab, de_vocab = build_vocabularies(data_dir, min_freq)
    print(f"EN vocab size: {len(en_vocab)}, DE vocab size: {len(de_vocab)}")

    # 数据
    train_dl = DataLoader(
        TranslationDataset(f"{data_dir}/train.jsonl", en_vocab, de_vocab),
        batch_size=batch_size, shuffle=True, collate_fn=collate_fn,
    )
    val_dl = DataLoader(
        TranslationDataset(f"{data_dir}/val.jsonl", en_vocab, de_vocab),
        batch_size=batch_size, collate_fn=collate_fn,
    )

    # 模型
    model = Transformer(
        src_vocab_size=len(en_vocab), tgt_vocab_size=len(de_vocab),
        dim=dim, heads=heads, enc_layers=enc_layers, dec_layers=dec_layers,
        dropout=dropout, max_len=max_len,
    ).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr, betas=(0.9, 0.98), eps=1e-9)
    criterion = nn.CrossEntropyLoss(ignore_index=PAD_ID, label_smoothing=0.1)

    # Warmup + Noam 学习率调度（原论文方案）
    def noam_lr(step):
        step = max(step, 1)
        return min(step ** -0.5, step * warmup_steps ** -1.5) * (dim ** 0.5)

    scheduler = torch.optim.lr_scheduler.LambdaLR(optimizer, lr_lambda=noam_lr)

    # 训练
    print(f"Training on {device}...")
    best_val_loss = float("inf")
    for epoch in range(1, epochs + 1):
        train_loss = train_epoch(model, train_dl, optimizer, criterion, scheduler, device)
        val_loss = evaluate(model, val_dl, criterion, device)
        cur_lr = optimizer.param_groups[0]["lr"]
        print(f"Epoch {epoch:02d}/{epochs} | Train Loss: {train_loss:.4f} | Val Loss: {val_loss:.4f} | LR: {cur_lr:.2e}")

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            torch.save({"model": model.state_dict(), "en_vocab": en_vocab.word2idx, "de_vocab": de_vocab.word2idx}, save_path)
            print(f"  -> Saved best model (val_loss={val_loss:.4f})")

    # 翻译测试
    print("\nLoading best model for translation...")
    ckpt = torch.load(save_path, map_location=device, weights_only=False)
    model.load_state_dict(ckpt["model"])

    test_sentences = [
        "A man is standing in the rain.",
        "Two dogs are playing in the grass.",
        "A woman is reading a book on the bench.",
        "Three children are running in the park.",
        "A cat is sitting on a chair.",
    ]
    print("\n--- Translation Results ---")
    for sent in test_sentences:
        result = translate(model, sent, en_vocab, de_vocab, device)
        print(f"EN: {sent}\nDE: {result}\n")

```