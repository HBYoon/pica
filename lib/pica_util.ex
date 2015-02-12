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
  def scan_right(pica func) do
    scan_right(pica, 250, func)
  end
  
  def scan_right(pica, unit, func) when 0 < unit do
    startPos = Pica.last_pk(pica)
    unit = unit - 1
    
    do_scan_right(startPos, pica, unit, [], func)
  end
  
  defp do_scan_right(-1,_pica,_unit, result,_func) do
    result
  end
  
  defp do_scan_right(current, pica, unit, result, func) do
    to = max((current - unit), 0)
    range = {current, to}
    
    case scan(pica, range, result, func) do
      {:error, reason} -> {:error, reason}
      {:stop, result} -> {:stop, result}
      result -> do_scan_right(to - 1, pica, unit, result, func)
    end
  end
  
  
  
  @doc"""
    scan from first appended elements
  """
  def scan_left(pica, func) do
    scan_left(pica, 250, func)
  end
  
  def scan_left(pica, unit, func) do
    endPos = Pica.last_pk(pica)
    unit = unit - 1
    
    do_scan_left(0, endPos, pica, unit, [], func)
  end
  
  defp do_scan_left(current, endPos,_pica,_unit, result,_func) when endPos < current do
    result
  end
  
  defp do_scan_left(current, endPos, pica, unit, result, func) do
    to = min((current + unit), endPos)
    range = {current, to}
    case scan(pica, range, result, func) do
      {:error, reason} -> {:error, reason}
      {:stop, result} -> result
      result -> do_scan_left(to + 1, endPos, pica, unit, result, func)
    end
  end
  
  
  
  @doc"""
    scan selected elements
  """
  def scan(pica, range, func) do
    scan(pica, range, [], func)
  end
  
  def scan(pica, range, firstArg, func) do
    case get(pica, range) do
      {:error, reason} -> {:error, reason}
      {:ok, binList} -> do_scan(binList, firstArg, func)
    end
  end
  
  defp do_scan([h|t], result, func) do
    next = fn(nextResult) -> do_scan(t, nextResult, func) end
    func.(h, result, next)
  end
  defp do_scan([], result, _func) do
    result
  end
  
  
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
