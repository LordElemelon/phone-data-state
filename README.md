# Dataset
Dataset je preuzet sa UCI repozitorijuma, i dostupan je na linku: http://archive.ics.uci.edu/ml/datasets/Heterogeneity+Activity+Recognition
Dataset sadrži očitavanje podataka akcelerometra i žiroskopa 9 različitih korisnika tokom jednog dana, i tim podacima pridruženu informaciju o vremenu dana u milisekundama, model telefona sa kojeg je podatak očitan, kao i u kojem stanju je zabeleženo da je korisnik bio u datom trenutku. Moguća stanja su: sedenje, stajanje, šetanje, penjanje uz stepenice, silaženje niz stepenice, kao i voženje bicikla.
# Batch obrada
Cilj batch obrade je pripremiti podatke za treniranje algoritma, istrenirati algoritam, i sačuvati ga u memoriju za dalje korišćenje. Obrada podataka se radi tako što se podaci uzeti sa oba uređaja podele na intervale od 5 sekundi, a onda za svaki interval izračunati trend podataka linearnom regresijom, kao i grešku intervala u odnosu na linearnu regresiju. Brojčano predstavljena greška je bitna jer daje informaciju i tome koliko merenje uređaja osciluje. Za svaki interval se pridružuje i podatak o prosečnim vrednostima merenja, i na kraju se podaci svih intervala šalju u Random Forest algoritam za treniranje.
# Streaming obrada
Cilj streaming obrade je da se podučeni algoritam koristi u simuliranom realnom okruženju. Obuhvataju se podaci svakih 5 sekundi sa uređaja korisnika, pripremaju se istim linearnim regresijama kao i tokom batch obrade, i zatim ubacuju kroz istreniran Random Forest model učitan iz memorije. Na ovaj način imamo uvid u stanje korisnika povezanih na sistem, kao neki vid nadgledanja fizičke aktivnosti skupa ljudi.
