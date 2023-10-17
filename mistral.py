# from transformers import AutoModelForCausalLM, AutoTokenizer
from ctransformers import AutoModelForCausalLM

device = "cpu" # the device to load the model onto

llm = AutoModelForCausalLM.from_pretrained("TheBloke/Mistral-7B-Instruct-v0.1-GGUF")

print(llm("<s>[INST] For the following publication title 'Parent of Origin gene expression in the bumblebee, Bombus terrestris, supports Haig's kinship theory for the evolution of genomic imprinting', the research keywords are listed below. [/INST]"))

exit()

model = AutoModelForCausalLM.from_pretrained("mistralai/Mistral-7B-Instruct-v0.1")
tokenizer = AutoTokenizer.from_pretrained("mistralai/Mistral-7B-Instruct-v0.1")

messages = [
    {"role": "user", "content": "For the following publication title 'Parent of Origin gene expression in the bumblebee, Bombus terrestris, supports Haig's kinship theory for the evolution of genomic imprinting', the research questions are listed below."}
]

encodeds = tokenizer.apply_chat_template(messages, return_tensors="pt")

model_inputs = encodeds.to(device)
model.to(device)

generated_ids = model.generate(model_inputs, max_new_tokens=1000, do_sample=True)
decoded = tokenizer.batch_decode(generated_ids)
print(decoded[0])