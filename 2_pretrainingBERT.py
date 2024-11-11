################# For running pre-training

from transformers import BertTokenizer, DataCollatorForLanguageModeling, BertForMaskedLM, TrainingArguments, Trainer, logging
import torch
# import tensorflow as tf
import pandas as pd
from datasets import Dataset
import multiprocessing
import numpy as np
import evaluate
import re

logging.set_verbosity_info()


# Set up GPU backend
# https://pytorch.org/docs/main/notes/mps.html
# https://stackoverflow.com/questions/63423463/using-pytorch-cuda-on-macbook-pro
if torch.backends.mps.is_available():
    mps_device = torch.device("mps")
    x = torch.ones(1, device=mps_device)
    print (x)
    # output expected:
    # tensor([1.], device='mps:0')

else:
    print ("MPS device not found.")

# Read in events, clean and store as dataset
# https://towardsdatascience.com/does-bert-need-clean-data-part-2-classification-d29adf9f745a
def text_clean(x):

    ### Light
    x = x.lower() # lowercase everything
    x = x.encode('ascii', 'ignore').decode()  # remove unicode characters
    x = re.sub(r'https*\S+', ' ', x) # remove links
    x = re.sub(r'http*\S+', ' ', x)
    # cleaning up text
    x = re.sub(r'\'\w+', '', x) 
    x = re.sub(r'\w*\d+\w*', '', x)
    x = re.sub(r'\s{2,}', ' ', x)
    x = re.sub(r'\s[^\w\s]\s', '', x)
    
    # ### Heavy
    # x = ' '.join([word for word in x.split(' ') if word not in stopwords])
    # x = re.sub(r'@\S', '', x)
    # x = re.sub(r'#\S+', ' ', x)
    # x = re.sub('[%s]' % re.escape(string.punctuation), ' ', x)
    # # remove single letters and numbers surrounded by space
    # x = re.sub(r'\s[a-z]\s|\s[0-9]\s', ' ', x)

    return x
df = pd.read_csv('eid_eventText.csv', usecols=['eid','event_text']).dropna(how='any',axis=0)
print(len(df))
df.drop_duplicates(subset=['event_text'],keep='first',inplace=True)
print(len(df))
df['event_text'] = df.event_text.apply(text_clean)
df = df[df['event_text'].str.split().str.len().gt(20)] # drops events with fewer than 20 words  
df.rename(columns={"event_text":"text"},inplace=True)
print(len(df))

dataset = Dataset.from_pandas(df).shuffle(seed=242)

# Tokenization
tokenizer = BertTokenizer.from_pretrained("bert-base-uncased")

def tokenize_function(reports):
    return tokenizer(reports["text"], return_tensors="np", truncation=True, padding="max_length")

tokenized_ds = dataset.map(tokenize_function, batched=True, num_proc=multiprocessing.cpu_count())

# Model configuration
model = BertForMaskedLM.from_pretrained('bert-base-uncased')#,
                                        # torch_dtype=torch.float16, attn_implementation="sdpa", # these are for efficiency
                                        # num_labels=2 # https://github.com/huggingface/transformers/issues/27707
                                        # )
# model = BertForMaskedLM.from_pretrained('./event_trainer/checkpoint-43500') 

# Evaluation 
metric = evaluate.load("accuracy")
def compute_metrics(eval_pred):
    labels = eval_pred.label_ids
    preds = eval_pred.predictions.argmax(axis = -1)
    for i in range(0,len(labels)):
        metric.add_batch(predictions=preds[i], references=labels[i])
    # return metric.compute(predictions=preds, references=labels)
    return metric.compute()

# Data
train_eval_ds = tokenized_ds.train_test_split(test_size=0.1, shuffle=True,seed=42)
data_collator = DataCollatorForLanguageModeling(tokenizer=tokenizer, mlm_probability=0.15)

# Trainer
training_args = TrainingArguments(
    output_dir="event_trainer4", 
    eval_strategy="epoch",
    eval_accumulation_steps = 16, # avoid OOM error, but putting preditictions during evalauation on the CPU
    per_device_eval_batch_size = 1, # This fixes the array size > 2**32 error if more than one batch is left on GPU
    num_train_epochs = 5,#, # default is 3,
    do_eval = True
    )

small_eval_dataset = train_eval_ds['test'].select(list(range(0,1000)))
# small_train_dataset = train_eval_ds["train"].select(list(range(0,1000)))
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_eval_ds["train"],
    # train_dataset=small_train_dataset,
    eval_dataset = small_eval_dataset,
    # eval_dataset = train_eval_ds["test"],
    data_collator=data_collator,
    compute_metrics=compute_metrics
)

trainer.train(resume_from_checkpoint = True)
# trainer.train()
