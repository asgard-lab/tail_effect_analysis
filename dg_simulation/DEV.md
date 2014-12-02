# Resumo

## tools.py
* carrega as informações do host

##client.py
* Potencia das Máquinas Heterogêneas: Segue uma distribuição randomica log normal (random.normalvariate) do python utilizando a potência base como média _mu_ e 40% da potência base como desvio padrão _sigma_

```python
if random:
    self.power=int(random.normalvariate(power, power / 2.5))
    if self.power > power * 3:
        self.power = power * 3
    if self.power < power / 3:
        self.power = power / 3
    if self.power <= 0:
        self.power = 1
```
