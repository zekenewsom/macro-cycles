import { Card } from "@/components/ui/card";
import ReactMarkdown from "react-markdown";
import { j } from "@/lib/api";

export const dynamic = "force-dynamic";

export default async function Page() {
  const note = await j<{ markdown: string; generated_at?: number }>("/note/monthly");
  return (
    <div className="p-6 space-y-6">
      <Card className="p-6 prose prose-invert max-w-none">
        <ReactMarkdown>{note.markdown}</ReactMarkdown>
      </Card>
    </div>
  );
}

