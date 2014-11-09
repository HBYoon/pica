defmodule Pica.Util do
  
  require Pica
  
  alias :lists, as: L
  
  
  
  def sync(Pica.pica_rec() = target, Pica.pica_rec(file: fd)) do
    Pica.pica_rec(target, [file: fd])
  end
  
  def sync(Pica.pica_rec() = target, fd) do
    Pica.pica_rec(target, [file: fd])
  end
  
  
  
  @doc"""
    scan from last appended elements
  """
  def scan_right(func, pica) do
    scan_right(func, pica, 250)
  end
  
  def scan_right(func, pica, unit) when 0 < unit do
    startPos = Pica.last_pk(pica)
    unit = unit - 1
    
    do_scan_right(startPos, func, pica, unit, [])
  end
  
  defp do_scan_right(-1,_func,_pica,_unit, result) do
    {:ok, result}
  end
  
  defp do_scan_right(current, func, pica, unit, result) do
    to = max((current - unit), 0)
    range = {current, to}
    
    case scan(func, pica, range, result) do
      {:error, reason} -> {:error, reason}
      {:stop, result} -> {:stop, result}
      {:ok, result} -> do_scan_right(to - 1, func, pica, unit, result)
    end
  end
  
  
  
  @doc"""
    scan from first appended elements
  """
  def scan_left(func, pica) do
    scan_left(func, pica, 250)
  end
  
  def scan_left(func, pica, unit) do
    endPos = Pica.last_pk(pica)
    unit = unit - 1
    
    do_scan_left(0, endPos, func, pica, unit, [])
  end
  
  defp do_scan_left(current, endPos,_func,_pica,_unit, result) when endPos < current do
    {:ok, result}
  end
  
  defp do_scan_left(current, endPos, func, pica, unit, result) do
    to = min((current + unit), endPos)
    range = {current, to}
    case scan(func, pica, range, result) do
      {:error, reason} -> {:error, reason}
      {:stop, result} -> {:stop, result}
      {:ok, result} -> do_scan_left(to + 1, endPos, func, pica, unit, result)
    end
  end
  
  
  
  @doc"""
    scan selected elements
  """
  def scan(func, pica, range) do
    scan(func, pica, range, [])
  end
  
  def scan(func, pica, range, firstArg) do
    case get(pica, range) do
      {:error, reason} -> {:error, reason}
      {:ok, binList} -> do_scan(binList, func, firstArg)
    end
  end
  
  defp do_scan([h|t], func, result), 
    do: func.(h, result) |> do_scan(t, func)
  defp do_scan([],_func, result), 
    do: {:ok, result}
    
  defp do_scan({:ok, result}, bList, func),
    do: do_scan(bList, func, result)
  defp do_scan({:stop, result},_bl,_f),
    do: {:stop, result}
  
  
  defp get(pica, {from, to}) when to < from do
    case Pica.get(pica, {to, from}) do 
      {:ok, result} -> 
        {:ok, L.reverse(result)}
      {:error, reason} -> 
        {:error, reason}
    end
  end
  
  defp get(pica, range) do
    Pica.get(pica, range)
  end
  
  
  
end
