import { useState, useEffect } from "react";
import { Search, AlertCircle, Building2, Loader2 } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { toast } from "@/hooks/use-toast";
import { supabase } from "@/integrations/supabase/client";
import { TerritoryData } from "@/types/territory";
import { mapDbToTerritoryData } from "@/utils/territoryMapper";
import { z } from "zod";

interface SearchSirenProps {
  onTerritoryFound: (territory: TerritoryData) => void;
}

const searchSchema = z.string().trim().min(1).max(100);

export const SearchSiren = ({ onTerritoryFound }: SearchSirenProps) => {
  const [siren, setSiren] = useState("");
  const [isSearching, setIsSearching] = useState(false);
  const [suggestions, setSuggestions] = useState<{ code_siren: string; name: string }[]>([]);
  const [showSuggestions, setShowSuggestions] = useState(false);

  useEffect(() => {
    const fetchSuggestions = async () => {
      if (siren.length < 2) {
        setSuggestions([]);
        setShowSuggestions(false);
        return;
      }

      const { data, error } = await supabase
        .from("territories")
        .select("code_siren, name")
        .or(`name.ilike.%${siren}%,code_siren.ilike.%${siren}%`)
        .limit(8);

      if (!error && data) {
        setSuggestions(data);
        setShowSuggestions(data.length > 0);
      }
    };

    const debounce = setTimeout(fetchSuggestions, 200);
    return () => clearTimeout(debounce);
  }, [siren]);

  const handleSearch = async () => {
    const validation = searchSchema.safeParse(siren);
    if (!validation.success) {
      toast({
        title: "Champ vide",
        description: "Veuillez entrer un numéro SIREN ou un nom d'EPCI.",
        variant: "destructive",
      });
      return;
    }

    setIsSearching(true);
    setShowSuggestions(false);

    const searchTerm = validation.data;

    const { data, error } = await supabase
      .from("territories")
      .select("*")
      .or(`code_siren.eq.${searchTerm},name.ilike.%${searchTerm}%`)
      .limit(1)
      .maybeSingle();

    if (error) {
      toast({
        title: "Erreur",
        description: "Une erreur est survenue lors de la recherche.",
        variant: "destructive",
      });
      setIsSearching(false);
      return;
    }

    if (data) {
      onTerritoryFound(mapDbToTerritoryData(data));
      toast({
        title: "EPCI trouvé",
        description: `${data.name} a été trouvé.`,
      });
    } else {
      toast({
        title: "EPCI non trouvé",
        description: "Aucun territoire ne correspond à votre recherche.",
        variant: "destructive",
      });
    }

    setIsSearching(false);
  };

  const handleSelectSuggestion = async (epci: { code_siren: string; name: string }) => {
    setSiren(epci.name);
    setShowSuggestions(false);

    const { data } = await supabase
      .from("territories")
      .select("*")
      .eq("code_siren", epci.code_siren)
      .maybeSingle();

    if (data) {
      onTerritoryFound(mapDbToTerritoryData(data));
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      handleSearch();
    }
  };

  return (
    <div className="w-full max-w-xl">
      <div className="relative">
        <div className="flex gap-2">
          <div className="relative flex-1">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-muted-foreground" />
            <Input
              type="text"
              placeholder="Rechercher par SIREN ou nom d'EPCI..."
              value={siren}
              onChange={(e) => setSiren(e.target.value)}
              onKeyPress={handleKeyPress}
              onFocus={() => siren.length >= 2 && suggestions.length > 0 && setShowSuggestions(true)}
              onBlur={() => setTimeout(() => setShowSuggestions(false), 200)}
              className="pl-10 h-12 text-base bg-card border-border"
              maxLength={100}
            />
          </div>
          <Button onClick={handleSearch} disabled={isSearching} className="h-12 px-6">
            {isSearching ? <Loader2 className="w-4 h-4 animate-spin" /> : "Rechercher"}
          </Button>
        </div>

        {/* Suggestions dropdown */}
        {showSuggestions && suggestions.length > 0 && (
          <div className="absolute top-full left-0 right-12 mt-1 bg-card border border-border rounded-lg shadow-lg z-50 max-h-64 overflow-y-auto">
            {suggestions.map((epci) => (
              <button
                key={epci.code_siren}
                onClick={() => handleSelectSuggestion(epci)}
                className="w-full px-4 py-2.5 text-left hover:bg-muted flex items-center gap-3 border-b border-border/50 last:border-0"
              >
                <Building2 className="w-4 h-4 text-muted-foreground flex-shrink-0" />
                <div className="min-w-0 flex-1">
                  <p className="text-sm font-medium text-foreground truncate">{epci.name}</p>
                  <p className="text-xs text-muted-foreground">SIREN: {epci.code_siren}</p>
                </div>
              </button>
            ))}
          </div>
        )}
      </div>

      <p className="text-xs text-muted-foreground mt-2 flex items-center gap-1">
        <AlertCircle className="w-3 h-3" />
        Cliquez sur la carte pour sélectionner une intercommunalité
      </p>
    </div>
  );
};
