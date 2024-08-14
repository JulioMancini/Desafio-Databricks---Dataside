# Desafio-Databricks-Freelancer

Spark Config: 
"spark.databricks.delta.schema.autoMerge.enabled": "true"
"spark.cleaner.periodicGC.interval": "10"
"spark.databricks.delta.autoCompact.enabled": "auto"
"spark.databricks.delta.optimizeWrite.enabled": "true"

5- Criar uma CMD e adicionar o código de create table contido no script_create_table.
6 – Criar outra CMD e adicionar o código de insert into dos registros contido no script script_insert_into.
7 – Criar uma única query para tratar todos as regras de negócio citadas abaixo. Todas as regras devem ser tratadas em UMA ÚNICA consulta SQL, ou seja, cada regra não pode estar em consultas separadas.
8 – O resultado final será um select com todos os dados tratados atendendo as regras citadas. 
Perguntas e Regras de Negócio
⦁	Regra de Negócio: Idade
⦁	Pergunta: Alguns registros contêm idades negativas. Crie uma lógica para substituir idades negativas por NULL.
⦁	Regra: idade < 0 deve ser substituída por NULL.
⦁	Regra de Negócio: Email
⦁	Pergunta: Alguns emails não possuem um formato válido. Crie uma lógica para definir como NULL os emails que não contêm o símbolo @.
⦁	Regra: Emails sem @ devem ser substituídos por NULL.
⦁	Regra de Negócio: Telefone
⦁	Pergunta: Verifique se todos os números de telefone possuem exatamente 10 dígitos. Se não, defina esses valores como NULL.
⦁	Regra: Telefones que não têm exatamente 10 dígitos devem ser substituídos por NULL.
⦁	Regra 2: Formate os telefones corretos para o layout (XX) AAAA-BBBB.
⦁	Regra de Negócio: CEP
⦁	Pergunta: Verifique se os CEPs estão no formato correto (00000-000) e se contêm apenas números. Se não, substitua-os por 00000-000.
⦁	Regra: CEPs que não estão no formato correto ou contêm caracteres não numéricos devem ser substituídos por 00000-000.
⦁	Regra de Negócio: Data de Contratação
⦁	Pergunta: Algumas datas de contratação não são válidas. Crie uma lógica para definir essas datas como 1900-00-00 e converta os valores válidos para o tipo DATE. O formato correto da data é AAAA-MM-DD.
⦁	Regra: Datas de contratação que não são válidas devem ser substituídas por 1900-00-00. O formato correto é AAAA-MM-DD.
⦁	Regra de Negócio: Data de Nascimento
⦁	Pergunta: Verifique se todas as datas de nascimento são válidas. Se não, defina essas datas como 1900-00-00 e converta os valores válidos para o tipo DATE. O formato correto da data é AAAA-MM-DD. Calcule a idade baseada na data de nascimento e a data atual.
⦁	Regra: Datas de nascimento que não são válidas devem ser substituídas por 1900-00-00. O formato correto é AAAA-MM-DD. A idade deve ser calculada com base na data de nascimento e a data atual.
⦁	Regra de Negócio: CPF
⦁	Pergunta: Verifique se os CPFs contêm exatamente 11 dígitos numéricos. Se não, substitua-os por 000.000.000-00. Formate os CPFs válidos para o formato 000.000.000-00.
⦁	Regra: CPFs que não contêm exatamente 11 dígitos numéricos ou contêm letras devem ser substituídos por 000.000.000-00. CPFs válidos devem ser formatados como 000.000.000-00.
⦁	Regra de Negócio: Salário
⦁	Pergunta: Alguns registros possuem salários negativos. Crie uma lógica para definir esses salários como NULL.
⦁	Regra: Salários negativos devem ser substituídos por NULL.
⦁	Regra de Negócio: Pontuação de Crédito
⦁	Pergunta: Verifique se as pontuações de crédito estão dentro do intervalo aceitável (300 a 850). Se não, defina esses valores como NULL.
⦁	Regra: Pontuações de crédito fora do intervalo 300-850 devem ser substituídas por NULL.
⦁	Regra de Negócio: Dívida
⦁	Pergunta: Alguns registros possuem valores de dívida negativos. Crie uma lógica para definir essas dívidas como NULL.
⦁	Regra: Dívidas negativas devem ser substituídas por NULL.
⦁	Regra de Negócio: Gênero
⦁	Pergunta: Verifique se todos os gêneros são 'M' ou 'F'. Se não, defina esses valores como NULL.
⦁	Regra: Gêneros que não são 'M' ou 'F' devem ser substituídos por NULL.
⦁	Regra de Negócio: Estado
⦁	Pergunta: Remova espaços em branco antes e depois dos nomes dos estados e crie uma nova coluna com o nome completo do estado com base na sigla.
⦁	Regra: Espaços em branco devem ser removidos e uma nova coluna deve ser criada com o nome completo do estado com base na sigla.
⦁	Regra de Negócio: País
⦁	Pergunta: Verifique se todos os países são válidos e corrigidos para 'Brasil'. Se não, substitua-os por NULL.
⦁	Regra: Países que não são 'Brasil' ou estão incorretos devem ser substituídos ou corrigidos para 'Brasil'.
⦁	Regra de Idade:
⦁	Pergunta: Para datas válidas, criar uma coluna de idade_real, que é a diferença do ano da data atual menos o ano da data de nascimento.
⦁	Regra: Calcule a idade baseada na data de nascimento e a data atual.

**Datos para o teste**


select id
     CREATE OR REPLACE TABLE default.dados_brutos (
    id INT,
    nome VARCHAR(255),
    idade INT,
    email VARCHAR(255),
    telefone VARCHAR(50),
    endereco VARCHAR(255),
    cidade VARCHAR(100),
    estado VARCHAR(50),
    cep VARCHAR(20),
    pais VARCHAR(100),
    empresa VARCHAR(255),
    cargo VARCHAR(100),
    salario INT,
    data_contratacao STRING,
    data_nascimento STRING,
    cpf VARCHAR(20),
    cartao_credito VARCHAR(20),
    pontuacao_credito INT,
    divida INT,
    genero VARCHAR(10)
);

INSERT INTO default.dados_brutos (id, nome, idade, email, telefone, endereco, cidade, estado, cep, pais, empresa, cargo, salario, data_contratacao, data_nascimento, cpf, cartao_credito, pontuacao_credito, divida, genero) VALUES
(1, 'João Silva', 29, 'joaosilva@', '1234567890', 'Rua Principal, 123', 'São Paulo', ' SP ', '12000-000', 'Brasil', 'Acme Corp', 'Engenheiro', 70000, '2021-15-01', '1992-22-03', '12345678901', '1234567890123456', 700, -5000, 'M'),
(2, 'Maria Oliveira', -25, 'mariaoliveira@exemplo.com', '5555555555', 'Rua Secundária, 456', 'Rio de Janeiro', ' RJ', '20001-000', 'Brasil', 'Beta Corp', 'Gerente', -90000, '2020-20-02', '1985-06-32', '98765432101', '9876543210987654', 650, 3000, 'F'),
(3, '', 35, 'alice@exemplo.com', '0000000000', 'Rua das Flores, 789', 'Curitiba', ' PR ', '80000-000', 'Brazil', 'Gamma Corp', '', 120000, '2019-03-40', '1988-15-08', '13579135791', '1357913579135791', 800, 10000, ''),
(4, 'Carlos Santos', 45, 'carlossantos@invalido', '6543216543', 'Avenida Paulista, 321', 'São Paulo', 'SP', '01310-000', 'Brasiil', 'Delta Corp', 'Analista', 50000, '2018-11-01', '1975-20-05', '24680246822', '2468024682246802', 600, -2000, 'M'),
(5, 'Ana Souza', 30, 'anasouza.com', '4444444444', 'Rua dos Pinheiros, 555', 'Belo Horizonte', 'MG', '30130-000', 'Brasill', 'Epsilon Corp', 'Consultora', 80000, '2020-05-50', '1990-10-10', '97531864239', '9753186423975318', 750, 5000, 'F'),
(6, 'Pedro Lima', -40, 'pedrolima@exemplo', '7777777777', 'Rua das Palmeiras, 666', 'Fortaleza', ' CE ', '60000-000', 'Brasil', 'Zeta Corp', 'Desenvolvedor', 0, '2022-03-14', '1980-25-07', '10293847561', '1029384756102938', 850, 0, ''),
(7, 'Lucia Alves', 50, 'luciaalves@@exemplo.com', '3333333333', 'Rua do Comércio, 777', 'Salvador', ' BA', '40000-000', 'Brasil', 'Theta Corp', 'Designer', -100000, '2017-09-30', '1970-15-01', '21354687952', '2135468795213546', 900, 20000, 'M'),
(8, 'Fernando Ribeiro', 28, 'fernandoribeiro@site', '2222222222', 'Rua das Acácias, 888', 'Porto Alegre', 'RS ', '90000-000', 'Brrazil', 'Iota Corp', 'Arquiteto', 65000, '2021-06-21', '1993-05-11', '32465798463', '3246579846324657', 500, -10000, 'F'),
(9, 'Gabriela Martins', 22, 'gabrielam@exemplo.com', '1111111111', 'Rua das Orquídeas, 999', 'Manaus', 'AM', '69000-000', 'Braziil', 'Kappa Corp', 'Enfermeira', 45000, '2019-12-11', '1998-02-28', '43576908714', '4357690871435769', 650, 0, ''),
(10, 'Hugo Ferreira', 33, 'hugoferreira@.com', '6666666666', 'Rua do Rosário, 123', 'Belém', ' InvalidState ', '66000-000', 'Braazil', 'Lambda Corp', 'Professor', -75000, '2016-08-08', '1987-17-09', '54687219095', '5468721909546872', 700, 15000, 'M'),
(11, 'Isabel Costa', 40, 'isabelc@exemplo.com', '0000000000', 'Rua do Carmo, 124', 'Recife', 'PE ', '50000-000', 'Brasiiil', 'Mu Corp', 'Contadora', 55000, '2021-01-12', '1981-04-05', '65798321106', '6579832110657983', 720, 500, 'F'),
(12, 'Joaquim Alves', 27, 'joaquima@dominio', '4444321234', 'Rua da Praia, 125', 'Vitória', ' ES', '29000-000', 'Brasil', 'Nu Corp', 'RH', 62000, '2020-07-23', '1994-06-12', '76809432197', '7680943219768094', 680, -3000, ''),
(13, 'Karina Rocha', 38, 'karinar@site.com', '3215555432', 'Rua das Laranjeiras, 126', 'Gotham', ' InvalidState ', '70000-000', 'InvalidCountry', 'Xi Corp', 'Marketing', -85000, '2018-04-11', '1983-30-05', '87920543218', '8792054321879205', 670, 7000, 'M'),
(14, 'Liam Brown', 24, 'liambrown@dominio.com', '1236666789', 'Rua dos Pinheiros, 127', 'Central City', ' InvalidState ', '90211-000', 'InvalidCountry', 'Omicron Corp', 'Vendas', 78000, '2019-06-19', '1997-22-03', '98031654329', '9803165432980316', 640, 300, 'F'),
(15, 'Mia Green', -32, 'miag@exemplo.com', '9871231234', 'Rua das Palmeiras, 128', 'Keystone', ' InvalidState ', '15002-000', 'InvalidCountry', 'Pi Corp', 'Suporte', 50000, '2021-08-29', '1989-05-12', '09142765430', '0914276543091427', 620, 8000, ''),
(16, 'Noah Blue', 26, 'noahblue@exemplo', '0000000000', 'Rua do Rosário, 129', 'Smallville', ' InvalidState ', '66003-000', 'InvalidCountry', 'Rho Corp', 'Engenheiro', 67000, '2020-14-02', '1995-11-11', '21053876542', '2105387654210538', 680, -1000, 'M'),
(17, 'Olivia Yellow', 31, 'oliviay@exemplo.com', '1115678111', 'Rua das Laranjeiras, 130', 'Star City', ' InvalidState ', '98102-000', 'InvalidCountry', 'Sigma Corp', 'Cientista', 90000, '2018-20-10', '1990-09-06', '32164987653', '3216498765321649', 650, 4000, 'F'),
(18, 'Paul Purple', 35, 'paulp@exemplo.com', '6663456543', 'Rua dos Pinheiros, 131', 'Coast City', ' InvalidState ', '33102-000', 'InvalidCountry', 'Tau Corp', 'Gerente', 56000, '2021-11-23', '1986-04-07', '43275098764', '4327509876432750', 600, -7000, ''),
(19, 'Quinn Violet', 42, 'quinnv@dominio', '3216789876', 'Rua do Comércio, 132', 'Gotham', ' InvalidState ', '70000-000', 'InvalidCountry', 'Upsilon Corp', 'Professor', -64000, '2020-05-13', '1979-08-14', '54386109875', '5438610987543861', 700, 2000, 'M'),
(20, 'Ryan Black', 37, 'ryanb@exemplo', '9874321234', 'Rua das Laranjeiras, 133', 'Metropolis', ' InvalidState ', '10004-000', 'InvalidCountry', 'Phi Corp', 'Desenvolvedor', 85000, '2019-12-30', '1984-10-03', '65497210986', '6549721098654972', 720, 500, 'F'),
(21, 'Sophia White', 29, 'sophiaw@exemplo.com', '5558765432', 'Rua das Palmeiras, 134', 'Central City', ' InvalidState ', '90212-000', 'InvalidCountry', 'Chi Corp', 'Designer', -53000, '2018-09-14', '1992-04-18', '76508321097', '7650832109765083', 650, 10000, ''),
(22, 'Tom Blue', 33, 'tomb@exemplo.com', '1119876543', 'Rua do Rosário, 135', 'Keystone', ' InvalidState ', '15003-000', 'InvalidCountry', 'Psi Corp', 'Analista', 47000, '2021-02-22', '1988-05-19', '87619432108', '8761943210876194', 690, -2000, 'M'),
(23, 'Uma Yellow', 26, 'umay@site.com', '6666543212', 'Rua das Laranjeiras, 136', 'Star City', ' InvalidState ', '98103-000', 'InvalidCountry', 'Omega Corp', 'RH', 53000, '2020-07-30', '1995-08-21', '98730543219', '9873054321987305', 710, 7000, 'F'),
(24, 'Vic Purple', 40, 'vicp@dominio.com', '4448765432', 'Rua dos Pinheiros, 137', 'Smallville', ' InvalidState ', '66004-000', 'InvalidCountry', 'Alpha Corp', 'Marketing', 65000, '2018-05-17', '1981-09-10', '09841765430', '0984176543098417', 620, -500, ''),
(25, 'Wendy Violet', 28, 'wendyv@exemplo.com', '3219876123', 'Rua do Comércio, 138', 'Coast City', ' InvalidState ', '33103-000', 'InvalidCountry', 'Beta Corp', 'Vendas', -72000, '2019-01-12', '1993-07-06', '10952876541', '1095287654109528', 680, 3000, 'M'),
(26, 'Xander Black', 31, 'xanderb@exemplo.com', '9876543333', 'Rua das Palmeiras, 139', 'Gotham', ' InvalidState ', '70005-000', 'InvalidCountry', 'Gamma Corp', 'Suporte', 58000, '2020-06-19', '1990-11-15', '21063987652', '2106398765210639', 700, -8000, 'F'),
(27, 'Yara White', -29, 'yaraw@dominio', '1238765432', 'Rua do Rosário, 140', 'Metropolis', ' InvalidState ', '10005-000', 'InvalidCountry', 'Delta Corp', 'Engenheira', 60000, '2021-04-21', '1992-30-03', '32174109873', '3217410987321741', 750, 500, ''),
(28, 'Zane Blue', 25, 'zaneb@site', '5556543333', 'Rua das Laranjeiras, 141', 'Central City', ' InvalidState ', '90213-000', 'InvalidCountry', 'Epsilon Corp', 'Enfermeiro', 49000, '2019-07-12', '1996-05-02', '43285210984', '4328521098432852', 680, -6000, 'M'),
(29, 'Anna Green', 39, 'annag@dominio.com', '4449876543', 'Rua dos Pinheiros, 142', 'Star City', ' InvalidState ', '98104-000', 'InvalidCountry', 'Zeta Corp', 'Cientista', -62000, '2020-08-18', '1982-12-29', '54396321095', '5439632109543963', 730, 1000, 'F'),
(30, 'Brian Yellow', 32, 'briany@site.com', '1116543212', 'Rua do Comércio, 143', 'Keystone', ' InvalidState ', '15004-000', 'InvalidCountry', 'Theta Corp', 'Gerente', 57000, '2021-03-25', '1989-07-11', '65407432106', '6540743210654074', 690, -300, 'M'),
(31, 'Lucas Branco', 25, 'lucas@dominio.com', '1111111111', 'Avenida Brasil, 200', 'Rio de Janeiro', ' RJ', '20000-ABC', 'Brasil', 'Omega Corp', 'Vendedor', 5000, '2022-10-10', '1997-08-12', '12345678A90', '1234567890123456', 600, 1000, 'M'),
(32, 'Eduardo Verde', 40, 'eduardo@dominio.com', '2222222222', 'Rua XV de Novembro, 300', 'São Paulo', 'SP ', '01000-0A0', 'Brazil', 'Alpha Corp', 'Engenheiro', 10000, '2020-05-20', '1982-12-20', '23456789B01', '2345678901234567', 700, 2000, 'M'),
(33, 'Clara Azul', 35, 'clara@dominio.com', '3333333333', 'Rua das Flores, 400', 'Curitiba', ' PR', '80000-0A0', 'Brasil', 'Beta Corp', 'Analista', 15000, '2019-03-15', '1986-04-01', '34567890C12', '3456789012345678', 800, 3000, 'F'),
(34, 'Beatriz Vermelho', 28, 'beatriz@dominio.com', '4444444444', 'Avenida Paulista, 500', 'São Paulo', ' SP ', '01310-0A0', 'Brasil', 'Gamma Corp', 'Consultor', 20000, '2018-11-11', '1993-07-17', '45678901D23', '4567890123456789', 900, 4000, 'F'),
(35, 'Fernando Amarelo', 42, 'fernando@dominio.com', '5555555555', 'Rua Augusta, 600', 'São Paulo', ' SP', '01400-0A0', 'Brazil', 'Delta Corp', 'Diretor', 25000, '2017-01-01', '1980-05-05', '56789012E34', '5678901234567890', 1000, 5000, 'M'),
(36, 'Carla Branco', 29, 'carla@dominio.com', '6666666666', 'Rua dos Pinheiros, 700', 'Belo Horizonte', ' MG ', '30130-0A0', 'Brasil', 'Epsilon Corp', 'Coordenador', 30000, '2021-05-05', '1992-10-10', '67890123F45', '6789012345678901', 1100, 6000, 'F'),
(37, 'Roberto Azul', 38, 'roberto@dominio.com', '7777777777', 'Rua das Palmeiras, 800', 'Fortaleza', ' CE ', '60000-0A0', 'Brasil', 'Zeta Corp', 'Gerente', 35000, '2022-03-03', '1984-07-07', '78901234G56', '7890123456789012', 1200, 7000, 'M'),
(38, 'Juliana Verde', 34, 'juliana@dominio.com', '8888888888', 'Rua do Comércio, 900', 'Salvador', 'BA ', '40000-0A0', 'Brazil', 'Theta Corp', 'Engenheiro', 40000, '2019-09-09', '1987-12-12', '89012345H67', '8901234567890123', 1300, 8000, 'F'),
(39, 'Felipe Vermelho', 27, 'felipe@dominio.com', '9999999999', 'Rua das Acácias, 1000', 'Porto Alegre', 'RS ', '90000-0A0', 'Brasil', 'Iota Corp', 'Desenvolvedor', 45000, '2020-12-12', '1995-02-02', '90123456I78', '9012345678901234', 1400, 9000, 'M'),
(40, 'Ana Rosa', 26, 'ana@dominio.com', '1111111111', 'Rua das Orquídeas, 1100', 'Manaus', ' AM ', '69000-0A0', 'Brazil', 'Kappa Corp', 'Analista', 50000, '2021-01-01', '1996-03-03', '01234567J89', '0123456789012345', 1500, 10000, 'F');


