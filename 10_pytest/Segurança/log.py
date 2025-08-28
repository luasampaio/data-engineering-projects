import win32evtlog

def consultar_log_erros():
    # Nome do log de eventos
    log_name = "System"  # Você pode alterar para "Application", "Security" ou outros logs disponíveis

    # Abrir o log de eventos
    hand = win32evtlog.OpenEventLog(None, log_name)

    # Definir o tipo de evento a ser consultado (neste caso, procuramos por erros)
    flags = win32evtlog.EVENTLOG_BACKWARDS_READ | win32evtlog.EVENTLOG_ERROR_TYPE

    # Começar a ler os eventos
    events = win32evtlog.ReadEventLog(hand, flags, 0)

    # Iterar sobre os eventos e imprimir os erros
    for event in events:
        if "Error" in event.StringInserts:
            print(f"Evento ID: {event.EventID & 0xFFFF}, Tipo: Error")
            print(f"Fonte: {event.SourceName}")
            print(f"Descrição: {event.StringInserts}\n")

    # Fechar o log de eventos
    win32evtlog.CloseEventLog(hand)

# Executar a função para consultar os erros no log de eventos
consultar_log_erros()

print(consultar_log_erros())