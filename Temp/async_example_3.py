import asyncio

async def msg(text):
    # эмитируем короткую 
    # задержку в выполнении
    await asyncio.sleep(0.1)
    print(text)

async def long_operation():
    # эмитируем долгую 
    # задержку в выполнении
    print('long_operation started')
    await asyncio.sleep(3)
    print('long_operation complete')

# основной цикл программы
async def main():
    # легкая сопрограмма
    await msg('1 msg complete')

    # Здесь запустим `long_operation()`, но ждать, пока она 
    # выполнится не хотим, т.к. необходимо получить второе 
    # сообщение как можно раньше. Для этого создаем для нее задачу... 
    task = asyncio.create_task(long_operation())
    #task = await long_operation()
    
    # легкая сопрограмма
    await msg('2 msg complete')

    # Теперь можно дождаться завершения 
    # задачи или отменить ее
    await task

if __name__ == "__main__":
    # запускаем цикл событий
    asyncio.run(main())

# 1 msg complete
# long_operation started
# 2 msg complete
# long_operation complete
