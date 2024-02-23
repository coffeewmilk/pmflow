
export default function RealtimeDash() {
    return (
        <>
        <div className="my-auto w-2/3">
                this is graph
            </div>
            <div className="flex flex-col h-full w-1/3 value-row-bg">
                <div className="flex h-16 header-bg">
                    <div className="my-auto"> header </div>
                </div>
                <div className="flex flex-col flex-nowrap space-y-4 overflow-y-auto">
                    <div className="flex h-10"> 
                        <p className="my-auto mx-2 w-3/5"> ThisStation </p>
                        <p className="my-auto w-1/5"> api </p>
                        <p className="my-auto mx-2 w-2/5"> time </p>
                    </div>

                </div>
            </div>
        </>
    )
}